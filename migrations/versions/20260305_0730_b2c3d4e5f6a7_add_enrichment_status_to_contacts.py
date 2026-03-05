"""Add enrichment_status and enrichment_notes to contacts

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-05 07:30:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "contacts",
        sa.Column(
            "enrichment_status",
            sa.String(length=20),
            nullable=True,
            comment="Enrichment status: enriched, needs_review, skipped",
        ),
    )
    op.add_column(
        "contacts",
        sa.Column(
            "enrichment_notes",
            sa.Text(),
            nullable=True,
            comment="Notes from enrichment automation (e.g., why human review is needed)",
        ),
    )


def downgrade() -> None:
    op.drop_column("contacts", "enrichment_notes")
    op.drop_column("contacts", "enrichment_status")
