"""Add work_type to companies and migrate sector data from company_type

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-03-05 08:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add work_type column
    op.add_column(
        "companies",
        sa.Column(
            "work_type",
            sa.String(length=255),
            nullable=True,
            comment="Type of work / market sectors (e.g., Corporate, Healthcare, Education)",
        ),
    )

    # Migrate data:
    # 'Corporate, Healthcare, Education' -> work_type, company_type becomes 'Owner'
    op.execute("""
        UPDATE companies
        SET work_type = company_type, company_type = 'Owner'
        WHERE company_type = 'Corporate, Healthcare, Education'
    """)

    # 'Real Estate Owner/Developer' -> work_type, company_type becomes 'Owner'
    op.execute("""
        UPDATE companies
        SET work_type = company_type, company_type = 'Owner'
        WHERE company_type = 'Real Estate Owner/Developer'
    """)

    # 'Third Party Service Provider' -> work_type, company_type becomes NULL
    op.execute("""
        UPDATE companies
        SET work_type = company_type, company_type = NULL
        WHERE company_type = 'Third Party Service Provider'
    """)


def downgrade() -> None:
    # Restore original company_type values from work_type
    op.execute("""
        UPDATE companies
        SET company_type = work_type
        WHERE work_type IN (
            'Corporate, Healthcare, Education',
            'Real Estate Owner/Developer',
            'Third Party Service Provider'
        )
    """)
    op.drop_column("companies", "work_type")
