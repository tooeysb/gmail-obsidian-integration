"""Add body column to emails table

Revision ID: 003_add_email_body
Revises: 002_relationship_profiles
Create Date: 2026-03-01 03:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "003_add_email_body"
down_revision = "002_relationship_profiles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("emails", sa.Column("body", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("emails", "body")
