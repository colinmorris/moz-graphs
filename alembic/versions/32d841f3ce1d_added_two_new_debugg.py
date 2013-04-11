"""added two new debugger fields

Revision ID: 32d841f3ce1d
Revises: 5463e2520d03
Create Date: 2013-04-11 11:05:46.787188

"""

# revision identifiers, used by Alembic.
revision = '32d841f3ce1d'
down_revision = '5463e2520d03'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bugmonths', sa.Column('assignee_constraint_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_outdegree_prior_month', sa.Integer(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_betweenness_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_clustering_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_betweenness_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_closeness_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_clustering_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_effectivesize_prior_month', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_indegree_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_indegree_prior_month', sa.Integer(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_effectivesize_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_closeness_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('assignee_outdegree_past_monthly_avg', sa.Float(), nullable=True))
    op.add_column('debuggers', sa.Column('bugtoucher', sa.Boolean(), nullable=True))
    op.add_column('debuggers', sa.Column('firstmonthid', sa.Integer(), nullable=True))
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('debuggers', 'firstmonthid')
    op.drop_column('debuggers', 'bugtoucher')
    op.drop_column('bugmonths', 'assignee_outdegree_past_monthly_avg')
    op.drop_column('bugmonths', 'assignee_closeness_past_monthly_avg')
    op.drop_column('bugmonths', 'assignee_effectivesize_past_monthly_avg')
    op.drop_column('bugmonths', 'assignee_indegree_prior_month')
    op.drop_column('bugmonths', 'assignee_indegree_past_monthly_avg')
    op.drop_column('bugmonths', 'assignee_effectivesize_prior_month')
    op.drop_column('bugmonths', 'assignee_clustering_prior_month')
    op.drop_column('bugmonths', 'assignee_closeness_prior_month')
    op.drop_column('bugmonths', 'assignee_betweenness_prior_month')
    op.drop_column('bugmonths', 'assignee_clustering_past_monthly_avg')
    op.drop_column('bugmonths', 'assignee_betweenness_past_monthly_avg')
    op.drop_column('bugmonths', 'assignee_outdegree_prior_month')
    op.drop_column('bugmonths', 'assignee_constraint_past_monthly_avg')
    ### end Alembic commands ###
