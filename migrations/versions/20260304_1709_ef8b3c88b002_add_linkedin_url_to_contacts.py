"""Add linkedin_url to contacts

Revision ID: ef8b3c88b002
Revises: e29bf653f294
Create Date: 2026-03-04 17:09:15.936117

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "ef8b3c88b002"
down_revision = "e29bf653f294"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "contacts",
        sa.Column(
            "linkedin_url",
            sa.String(length=500),
            nullable=True,
            comment="LinkedIn profile URL",
        ),
    )


def downgrade() -> None:
    op.drop_column("contacts", "linkedin_url")
