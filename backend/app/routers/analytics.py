"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    Returns a JSON array with four buckets:
    [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    """
    # Find the lab item by matching title (e.g., "lab-04" → "Lab 04")
    lab_number = lab.replace("lab-", "").replace("lab", "")
    lab_title_pattern = f"%Lab {lab_number}%"

    lab_item = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(lab_title_pattern),
        )
    )
    lab = lab_item.first()
    if lab is None:
        return [{"bucket": "0-25", "count": 0}, {"bucket": "26-50", "count": 0},
                {"bucket": "51-75", "count": 0}, {"bucket": "76-100", "count": 0}]

    # Find all tasks that belong to this lab
    task_ids = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab.id)
    )
    task_id_list = list(task_ids.all())

    if not task_id_list:
        return [{"bucket": "0-25", "count": 0}, {"bucket": "26-50", "count": 0},
                {"bucket": "51-75", "count": 0}, {"bucket": "76-100", "count": 0}]

    # Query interactions for these items that have a score
    bucket_0_25 = await session.exec(
        select(func.count()).where(
            InteractionLog.item_id.in_(task_id_list),
            InteractionLog.score.isnot(None),
            InteractionLog.score <= 25,
        )
    )
    bucket_26_50 = await session.exec(
        select(func.count()).where(
            InteractionLog.item_id.in_(task_id_list),
            InteractionLog.score.isnot(None),
            InteractionLog.score > 25,
            InteractionLog.score <= 50,
        )
    )
    bucket_51_75 = await session.exec(
        select(func.count()).where(
            InteractionLog.item_id.in_(task_id_list),
            InteractionLog.score.isnot(None),
            InteractionLog.score > 50,
            InteractionLog.score <= 75,
        )
    )
    bucket_76_100 = await session.exec(
        select(func.count()).where(
            InteractionLog.item_id.in_(task_id_list),
            InteractionLog.score.isnot(None),
            InteractionLog.score > 75,
            InteractionLog.score <= 100,
        )
    )

    return [
        {"bucket": "0-25", "count": bucket_0_25.one()},
        {"bucket": "26-50", "count": bucket_26_50.one()},
        {"bucket": "51-75", "count": bucket_51_75.one()},
        {"bucket": "76-100", "count": bucket_76_100.one()},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    Returns a JSON array:
    [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    """
    # Find the lab item by matching title
    lab_number = lab.replace("lab-", "").replace("lab", "")
    lab_title_pattern = f"%Lab {lab_number}%"

    lab_item = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(lab_title_pattern),
        )
    )
    lab = lab_item.first()
    if lab is None:
        return []

    # Find all tasks that belong to this lab
    tasks = await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab.id).order_by(ItemRecord.title)
    )

    result = []
    for task in tasks:
        # Query interactions for this task
        interactions = await session.exec(
            select(InteractionLog).where(InteractionLog.item_id == task.id)
        )
        interaction_list = interactions.all()

        attempts = len(interaction_list)
        if attempts > 0:
            scores = [i.score for i in interaction_list if i.score is not None]
            avg_score = round(sum(scores) / len(scores), 1) if scores else 0.0
        else:
            avg_score = 0.0

        result.append({
            "task": task.title,
            "avg_score": avg_score,
            "attempts": attempts,
        })

    return result


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    Returns a JSON array:
    [{"date": "2026-02-28", "submissions": 45}, ...]
    """
    # Find the lab item by matching title
    lab_number = lab.replace("lab-", "").replace("lab", "")
    lab_title_pattern = f"%Lab {lab_number}%"

    lab_item = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(lab_title_pattern),
        )
    )
    lab = lab_item.first()
    if lab is None:
        return []

    # Find all tasks that belong to this lab
    task_ids = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab.id)
    )
    task_id_list = list(task_ids.all())

    if not task_id_list:
        return []

    # Group interactions by date
    date_submissions = await session.exec(
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_id_list))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    return [{"date": row[0], "submissions": row[1]} for row in date_submissions.all()]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    Returns a JSON array:
    [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    """
    # Find the lab item by matching title
    lab_number = lab.replace("lab-", "").replace("lab", "")
    lab_title_pattern = f"%Lab {lab_number}%"

    lab_item = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(lab_title_pattern),
        )
    )
    lab = lab_item.first()
    if lab is None:
        return []

    # Find all tasks that belong to this lab
    task_ids = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab.id)
    )
    task_id_list = list(task_ids.all())

    if not task_id_list:
        return []

    # Join interactions with learners to get student_group
    # Group by student_group and compute avg_score and distinct students count
    query = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(InteractionLog.learner_id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_id_list))
        .where(InteractionLog.score.isnot(None))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    results = await session.exec(query)

    return [
        {
            "group": row[0],
            "avg_score": round(float(row[1]), 1) if row[1] is not None else 0.0,
            "students": row[2],
        }
        for row in results.all()
    ]
