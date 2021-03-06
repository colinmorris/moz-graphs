"""bug network

Revision ID: 14c49ab07397
Revises: 32d841f3ce1d
Create Date: 2013-04-17 10:15:55.737984

"""

# revision identifiers, used by Alembic.
revision = '14c49ab07397'
down_revision = '32d841f3ce1d'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bugmonths', sa.Column('bug_constraint_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_effective_size_churn_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_constraint_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_closeness_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_clustering_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_clustering_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_efficiency_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_effective_size_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_effective_size_churn_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_efficiency_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_closeness_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bug_effective_size_past_monthly_avg', sa.Float(), nullable=True))
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('bugmonths', 'bug_effective_size_past_monthly_avg')
    op.drop_column('bugmonths', 'bug_closeness_past_monthly_avg')
    op.drop_column('bugmonths', 'bug_efficiency_prior_month')
    op.drop_column('bugmonths', 'bug_effective_size_churn_prior_month')
    op.drop_column('bugmonths', 'bug_effective_size_prior_month')
    op.drop_column('bugmonths', 'bug_efficiency_past_monthly_avg')
    op.drop_column('bugmonths', 'bug_clustering_past_monthly_avg')
    op.drop_column('bugmonths', 'bug_clustering_prior_month')
    op.drop_column('bugmonths', 'bug_closeness_prior_month')
    op.drop_column('bugmonths', 'bug_constraint_past_monthly_avg')
    op.drop_column('bugmonths', 'bug_effective_size_churn_past_monthly_avg')
    op.drop_column('bugmonths', 'bug_constraint_prior_month')
    ### end Alembic commands ###
