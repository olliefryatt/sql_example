-- Table to do some quick analysis on onboarding date of new users.

-- Requirements: 
-- Want a table showing all onbaorded users with completed kyc
-- Their starting date, their current status
-- Info about the agent that onboarded them 

-- 1. Agent info
with df_1 as (
    select 
        unique_id as id_df_1,
        min(date) as min_date,
        max(date) as max_date,
        max(age) as age
    from metrics.user_classification
    group by unique_id
),

-- 2. Agent info + their current status
df_2 as (
    select 
        id_df_1 as agent_id,
        max_date,
        min_date,
        df_1.age as age,
        classification
    from df_1
    inner join metrics.user_classification on max_date = date
    and id_df_1 = unique_id
),

-- 3. Add agent KYC & their associated ABA id
df_3 as (
    select
        agent_id,
        "agentName",
        max_date,
        min_date,
        age,
        classification,
        "asaUuid"
    from df_2
    left join agent on agent_id = uuid
),

-- 4. Add in ABA name
df_4 as (
select
    agent_id,
    "agentName",
    max_date,
    min_date,
    age,
    classification,
    "asaUuid",
    name as aba_name,
    1 as number
from df_3
left join success_associate on "asaUuid" = success_associate.uuid
)

select *
from df_4
