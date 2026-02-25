"""Initial schema

Revision ID: 001_initial
Revises:
Create Date: 2026-02-25 22:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001_initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable pgcrypto extension for credential encryption
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto"')

    # Create users table
    op.create_table(
        'users',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('email', sa.String(255), nullable=False, unique=True),
        sa.Column('name', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    )
    op.create_index('ix_users_email', 'users', ['email'])

    # Create gmail_accounts table
    op.create_table(
        'gmail_accounts',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('account_email', sa.String(255), nullable=False),
        sa.Column('account_label', sa.String(50), nullable=False),
        sa.Column('credentials', postgresql.JSON, nullable=True),
        sa.Column('is_active', sa.Boolean, default=True, nullable=False),
        sa.Column('last_synced_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    )
    op.create_index('ix_gmail_accounts_user_id', 'gmail_accounts', ['user_id'])
    op.create_index('ix_gmail_accounts_account_email', 'gmail_accounts', ['account_email'])
    op.create_index('ix_gmail_accounts_account_label', 'gmail_accounts', ['account_label'])
    op.create_unique_constraint('uq_user_account_email', 'gmail_accounts', ['user_id', 'account_email'])

    # Create contacts table
    op.create_table(
        'contacts',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('email', sa.String(255), nullable=False),
        sa.Column('name', sa.String(255), nullable=True),
        sa.Column('phone', sa.String(50), nullable=True),
        sa.Column('account_sources', postgresql.ARRAY(sa.String), nullable=False, server_default='{}'),
        sa.Column('email_count', sa.Integer, default=0, nullable=False),
        sa.Column('notes', sa.Text, nullable=True),
        sa.Column('relationship_context', sa.String(50), nullable=True),
        sa.Column('last_contact_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    )
    op.create_index('ix_contacts_user_id', 'contacts', ['user_id'])
    op.create_index('ix_contacts_email', 'contacts', ['email'])
    op.create_unique_constraint('uq_user_contact_email', 'contacts', ['user_id', 'email'])

    # Create emails table
    op.create_table(
        'emails',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('account_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('gmail_accounts.id', ondelete='CASCADE'), nullable=False),
        sa.Column('gmail_message_id', sa.String(255), nullable=False),
        sa.Column('gmail_thread_id', sa.String(255), nullable=True),
        sa.Column('subject', sa.Text, nullable=True),
        sa.Column('sender_email', sa.String(255), nullable=False),
        sa.Column('sender_name', sa.String(255), nullable=True),
        sa.Column('recipient_emails', sa.Text, nullable=False),
        sa.Column('date', sa.DateTime(timezone=True), nullable=False),
        sa.Column('summary', sa.Text, nullable=True),
        sa.Column('has_attachments', sa.Boolean, default=False, nullable=False),
        sa.Column('attachment_count', sa.Integer, default=0, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    )
    op.create_index('ix_emails_user_id', 'emails', ['user_id'])
    op.create_index('ix_emails_account_id', 'emails', ['account_id'])
    op.create_index('ix_emails_date', 'emails', ['date'])
    op.create_index('ix_emails_sender_email', 'emails', ['sender_email'])
    op.create_unique_constraint('uq_account_message_id', 'emails', ['account_id', 'gmail_message_id'])

    # Create email_tags table
    op.create_table(
        'email_tags',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('email_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('emails.id', ondelete='CASCADE'), nullable=False),
        sa.Column('tag', sa.String(100), nullable=False),
        sa.Column('tag_category', sa.String(50), nullable=False),
        sa.Column('confidence', sa.Float, nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    )
    op.create_index('ix_email_tags_email_id', 'email_tags', ['email_id'])
    op.create_index('ix_email_tags_tag', 'email_tags', ['tag'])
    op.create_index('ix_email_tags_tag_category', 'email_tags', ['tag_category'])

    # Create sync_jobs table
    op.create_table(
        'sync_jobs',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('status', sa.String(50), nullable=False, server_default='queued'),
        sa.Column('phase', sa.String(50), nullable=True),
        sa.Column('progress_pct', sa.Integer, default=0, nullable=False),
        sa.Column('emails_processed', sa.Integer, default=0, nullable=False),
        sa.Column('emails_total', sa.Integer, nullable=True),
        sa.Column('contacts_processed', sa.Integer, default=0, nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('error_message', sa.Text, nullable=True),
        sa.Column('retry_count', sa.Integer, default=0, nullable=False),
        sa.Column('celery_task_id', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    )
    op.create_index('ix_sync_jobs_user_id', 'sync_jobs', ['user_id'])
    op.create_index('ix_sync_jobs_celery_task_id', 'sync_jobs', ['celery_task_id'])


def downgrade() -> None:
    op.drop_table('sync_jobs')
    op.drop_table('email_tags')
    op.drop_table('emails')
    op.drop_table('contacts')
    op.drop_table('gmail_accounts')
    op.drop_table('users')
    op.execute('DROP EXTENSION IF EXISTS "pgcrypto"')
