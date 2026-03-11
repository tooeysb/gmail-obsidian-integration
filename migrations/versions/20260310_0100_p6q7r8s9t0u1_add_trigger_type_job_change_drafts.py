"""Add trigger_type to draft_suggestions, make news_item_id nullable, add job_change_draft_generated_at.

Revision ID: p6q7r8s9t0u1
Revises: o5p6q7r8s9t0
Create Date: 2026-03-10
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "p6q7r8s9t0u1"
down_revision = "o5p6q7r8s9t0"
branch_labels = None
depends_on = None


def upgrade():
    # 1. Add trigger_type column with default for existing rows
    op.add_column(
        "draft_suggestions",
        sa.Column(
            "trigger_type",
            sa.String(20),
            nullable=False,
            server_default="news_mention",
            comment="Draft trigger: news_mention, job_change",
        ),
    )

    # 2. Make news_item_id nullable (for job_change drafts with no article)
    op.alter_column(
        "draft_suggestions",
        "news_item_id",
        existing_type=sa.dialects.postgresql.UUID(),
        nullable=True,
    )

    # 3. Drop old unique constraint and replace with partial indexes
    op.drop_constraint("uq_draft_news_contact", "draft_suggestions", type_="unique")

    # Partial unique index: one draft per news item + contact (when news_item_id is not null)
    op.create_index(
        "uq_draft_news_contact_v2",
        "draft_suggestions",
        ["news_item_id", "contact_id"],
        unique=True,
        postgresql_where=sa.text("news_item_id IS NOT NULL"),
    )

    # Partial unique index: one pending job change draft per contact
    op.create_index(
        "uq_draft_jobchange_contact",
        "draft_suggestions",
        ["contact_id", "trigger_type"],
        unique=True,
        postgresql_where=sa.text("trigger_type = 'job_change' AND status = 'pending'"),
    )

    # 4. Add job_change_draft_generated_at to contacts
    op.add_column(
        "contacts",
        sa.Column(
            "job_change_draft_generated_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When a job change outreach draft was auto-generated",
        ),
    )


def downgrade():
    op.drop_column("contacts", "job_change_draft_generated_at")
    op.drop_index("uq_draft_jobchange_contact", "draft_suggestions")
    op.drop_index("uq_draft_news_contact_v2", "draft_suggestions")

    # Restore original unique constraint (requires news_item_id to be non-null)
    op.create_unique_constraint(
        "uq_draft_news_contact", "draft_suggestions", ["news_item_id", "contact_id"]
    )

    # Make news_item_id non-nullable again (may fail if nulls exist)
    op.alter_column(
        "draft_suggestions",
        "news_item_id",
        existing_type=sa.dialects.postgresql.UUID(),
        nullable=False,
    )

    op.drop_column("draft_suggestions", "trigger_type")
