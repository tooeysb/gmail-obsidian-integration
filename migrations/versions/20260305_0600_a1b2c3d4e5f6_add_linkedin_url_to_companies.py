"""Add linkedin_url to companies

Revision ID: a1b2c3d4e5f6
Revises: ef8b3c88b002
Create Date: 2026-03-05 06:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "ef8b3c88b002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "companies",
        sa.Column(
            "linkedin_url",
            sa.String(length=500),
            nullable=True,
            comment="Company LinkedIn page URL",
        ),
    )


def downgrade() -> None:
    op.drop_column("companies", "linkedin_url")
