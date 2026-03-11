"""Add match_confidence to draft_suggestions.

Revision ID: q7r8s9t0u1v2
Revises: p6q7r8s9t0u1
Create Date: 2026-03-10
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "q7r8s9t0u1v2"
down_revision = "p6q7r8s9t0u1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "draft_suggestions",
        sa.Column(
            "match_confidence",
            sa.String(20),
            nullable=False,
            server_default="full_name",
            comment="Contact match quality: full_name, last_name",
        ),
    )


def downgrade():
    op.drop_column("draft_suggestions", "match_confidence")
