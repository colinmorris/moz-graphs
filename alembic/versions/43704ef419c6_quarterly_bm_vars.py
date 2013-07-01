"""quarterly bm vars

Revision ID: 43704ef419c6
Revises: 20dc3c4f8988
Create Date: 2013-06-30 15:28:49.495379

"""

# revision identifiers, used by Alembic.
revision = '43704ef419c6'
down_revision = '20dc3c4f8988'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bugmonths', sa.Column('bugs_debuggers_efficiency_past_quarterly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_indegree_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_outdegree_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_betweenness_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_effective_size_churn_quarterly_cumulative', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_effective_size_churn_past_quarterly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_alter_churn_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_betweenness_past_quarterly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_effective_size_past_quarterly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_effective_size_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_alter_churn_quarterly_cumulative', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_constraint_past_quarterly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_indegree_past_quarterly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_constraint_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_alter_churn_past_quarterly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_outdegree_past_quarterly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_closeness_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_closeness_past_quarterly_avg', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_clustering_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_effective_size_churn_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_efficiency_prior_quarter', sa.Float(), nullable=True))
    op.add_column('bugmonths', sa.Column('bugs_debuggers_clustering_past_quarterly_avg', sa.Float(), nullable=True))
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('bugmonths', 'bugs_debuggers_clustering_past_quarterly_avg')
    op.drop_column('bugmonths', 'bugs_debuggers_efficiency_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_effective_size_churn_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_clustering_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_closeness_past_quarterly_avg')
    op.drop_column('bugmonths', 'bugs_debuggers_closeness_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_outdegree_past_quarterly_avg')
    op.drop_column('bugmonths', 'bugs_debuggers_alter_churn_past_quarterly_avg')
    op.drop_column('bugmonths', 'bugs_debuggers_constraint_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_indegree_past_quarterly_avg')
    op.drop_column('bugmonths', 'bugs_debuggers_constraint_past_quarterly_avg')
    op.drop_column('bugmonths', 'bugs_debuggers_alter_churn_quarterly_cumulative')
    op.drop_column('bugmonths', 'bugs_debuggers_effective_size_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_effective_size_past_quarterly_avg')
    op.drop_column('bugmonths', 'bugs_debuggers_betweenness_past_quarterly_avg')
    op.drop_column('bugmonths', 'bugs_debuggers_alter_churn_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_effective_size_churn_past_quarterly_avg')
    op.drop_column('bugmonths', 'bugs_debuggers_effective_size_churn_quarterly_cumulative')
    op.drop_column('bugmonths', 'bugs_debuggers_betweenness_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_outdegree_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_indegree_prior_quarter')
    op.drop_column('bugmonths', 'bugs_debuggers_efficiency_past_quarterly_avg')
    ### end Alembic commands ###