from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase


def projects_coll(db: AsyncIOMotorDatabase) -> AsyncIOMotorCollection:
    return db["projects"]


def project_rows_coll(db: AsyncIOMotorDatabase) -> AsyncIOMotorCollection:
    return db["project_rows"]


def project_jobs_coll(db: AsyncIOMotorDatabase) -> AsyncIOMotorCollection:
    return db["project_jobs"]


def project_results_coll(db: AsyncIOMotorDatabase) -> AsyncIOMotorCollection:
    return db["project_results"]


async def ensure_indexes(db: AsyncIOMotorDatabase) -> None:
    await project_rows_coll(db).create_index([("project_id", 1), ("row_index", 1)], unique=True)
    await project_results_coll(db).create_index([("project_id", 1), ("row_index", 1)], unique=True)
    await project_jobs_coll(db).create_index([("project_id", 1)])
    await projects_coll(db).create_index([("updated_at", -1)])
    await projects_coll(db).create_index([("created_at", -1)])
