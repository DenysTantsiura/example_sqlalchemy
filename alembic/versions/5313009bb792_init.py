"""Init

Revision ID: 5313009bb792
Revises: 7d1ec268fef2
Create Date: 2023-02-24 15:22:36.505588

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5313009bb792'
down_revision = '7d1ec268fef2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('groups_',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('group_name', sa.CHAR(length=7), nullable=False),
    sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('group_name')
    )
    op.create_table('teachers',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.VARCHAR(length=50), nullable=False),
    sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name')
    )
    op.create_table('students',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.VARCHAR(length=50), nullable=False),
    sa.Column('group_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
    sa.ForeignKeyConstraint(['group_id'], ['groups_.id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name')
    )
    op.create_table('subjects',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('subject', sa.CHAR(length=40), nullable=False),
    sa.Column('teacher_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
    sa.ForeignKeyConstraint(['teacher_id'], ['teachers.id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('assessments',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('value_', sa.NUMERIC(), nullable=True),
    sa.Column('date_of', sa.DATE(), nullable=False),
    sa.Column('subject_id', sa.Integer(), nullable=True),
    sa.Column('student_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
    sa.ForeignKeyConstraint(['student_id'], ['students.id'], ),
    sa.ForeignKeyConstraint(['subject_id'], ['subjects.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('assessments')
    op.drop_table('subjects')
    op.drop_table('students')
    op.drop_table('teachers')
    op.drop_table('groups_')
    # ### end Alembic commands ###
