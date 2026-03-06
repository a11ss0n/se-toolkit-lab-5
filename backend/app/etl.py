"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    Uses HTTP Basic Auth with email/password from settings.
    Returns a list of dicts with keys: lab, task, title, type.
    Raises httpx.HTTPStatusError if status is not 200.
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)

    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination.

    Uses HTTP Basic Auth. Fetches logs in batches of 500.
    If 'since' is provided, only fetches logs submitted after that timestamp.
    Returns the combined list of all log dicts from all pages.
    """
    url = f"{settings.autochecker_api_url}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)
    all_logs: list[dict] = []

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if since is not None:
                params["since"] = since.isoformat()

            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Use the last log's submitted_at as the new since
            if logs:
                last_submitted_at = logs[-1].get("submitted_at")
                if last_submitted_at:
                    since = datetime.fromisoformat(last_submitted_at)
            else:
                break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    Processes labs first, then tasks with parent_id pointing to their lab.
    Returns the number of newly created items.
    """
    from app.models.item import ItemRecord
    from sqlmodel import select

    new_items_count = 0

    # Build a mapping from lab short_id (e.g., "lab-01") to db item id
    lab_id_to_item: dict[str, ItemRecord] = {}

    # Process labs first (items where type="lab")
    for item_data in items:
        if item_data.get("type") != "lab":
            continue

        title = item_data["title"]
        # Check if lab already exists
        existing = await session.exec(
            select(ItemRecord).where(ItemRecord.title == title)
        )
        item = existing.first()

        if item is None:
            # Create new lab item
            item = ItemRecord(
                type="lab",
                parent_id=None,
                title=title,
                description="",
            )
            session.add(item)
            new_items_count += 1

        # Map the short lab id (e.g., "lab-01") to the db record
        lab_short_id = item_data["lab"]
        lab_id_to_item[lab_short_id] = item

    # Commit labs so we can reference them
    await session.commit()
    for item in lab_id_to_item.values():
        await session.refresh(item)

    # Process tasks (items where type="task")
    for item_data in items:
        if item_data.get("type") != "task":
            continue

        title = item_data["title"]
        lab_short_id = item_data["lab"]
        parent_item = lab_id_to_item.get(lab_short_id)

        if parent_item is None:
            # Parent lab not found, skip this task
            continue

        # Check if task already exists with this title and parent_id
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.title == title,
                ItemRecord.parent_id == parent_item.id,
            )
        )
        task = existing.first()

        if task is None:
            # Create new task item
            task = ItemRecord(
                type="task",
                parent_id=parent_item.id,
                title=title,
                description="",
            )
            session.add(task)
            new_items_count += 1

    # Commit all inserts
    await session.commit()

    return new_items_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    Returns the number of newly created interactions.
    """
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner
    from sqlmodel import select

    # Build lookup: (lab_short_id, task_short_id) → item title
    # For labs: key is (lab, None)
    # For tasks: key is (lab, task)
    id_to_title: dict[tuple[str, str | None], str] = {}
    for item_data in items_catalog:
        lab_short_id = item_data["lab"]
        task_short_id = item_data.get("task")
        title = item_data["title"]
        id_to_title[(lab_short_id, task_short_id)] = title

    new_interactions_count = 0

    for log in logs:
        # 1. Find or create Learner by external_id
        student_id = str(log["student_id"])
        group = log.get("group", "")

        # Search by external_id (str), not by id (int)
        learner_result = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = learner_result.first()

        if learner is None:
            learner = Learner(
                external_id=student_id,
                student_group=group,
            )
            session.add(learner)
            await session.flush()  # Get the learner.id

        # 2. Find the matching item in the database
        lab_short_id = log["lab"]
        task_short_id = log.get("task")
        title = id_to_title.get((lab_short_id, task_short_id))

        if title is None:
            # No matching item found, skip this log
            continue

        # Query DB for ItemRecord with this title
        # For tasks, we also need to match parent_id
        item_statement = select(ItemRecord).where(ItemRecord.title == title)
        if task_short_id is not None:
            # For tasks, find by title and ensure it's a task (has parent_id)
            item_statement = item_statement.where(ItemRecord.type == "task")
        else:
            # For labs, ensure it's a lab (no parent_id)
            item_statement = item_statement.where(
                ItemRecord.type == "lab",
                ItemRecord.parent_id.is_(None),
            )

        item_result = await session.exec(item_statement)
        item = item_result.first()

        if item is None:
            # No matching item found, skip this log
            continue

        # 3. Check if InteractionLog with this external_id already exists
        external_id = log["id"]
        existing = await session.get(InteractionLog, external_id)
        if existing is not None:
            # Already exists, skip for idempotency
            continue

        # 4. Create InteractionLog
        from datetime import datetime

        submitted_at = log.get("submitted_at")
        created_at = (
            datetime.fromisoformat(submitted_at) if submitted_at else datetime.now()
        )

        interaction = InteractionLog(
            external_id=external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at,
        )
        session.add(interaction)
        new_interactions_count += 1

    # Commit all inserts
    await session.commit()

    return new_interactions_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    Returns a dict with:
    - new_records: number of new interactions created
    - total_records: total interactions in DB after sync
    """
    from app.models.interaction import InteractionLog
    from sqlmodel import select

    # Step 1: Fetch items from the API and load them
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine the last synced timestamp
    # Query the most recent created_at from InteractionLog
    latest_log = await session.exec(
        select(InteractionLog).order_by(InteractionLog.created_at.desc()).limit(1)
    )
    last_record = latest_log.first()
    since = last_record.created_at if last_record else None

    # Step 3: Fetch logs since that timestamp and load them
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    # Step 4: Get total records count
    total_result = await session.exec(select(InteractionLog))
    total_records = len(total_result.all())

    return {
        "new_records": new_records,
        "total_records": total_records,
    }
