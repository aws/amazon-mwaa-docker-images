-- Find active phone numbers that need reverse lookup.
-- Either never looked up or lookup is older than specified months
with
    active_leads as (
        select
            phone_number_e164
        from
            {{ params.trusted_database }}.SNAPSHOTS.SNAPSHOT_LEADS_AND_APPS_PHONE_NUMBERS
        where
            dbt_valid_to is null
    ),
    reverse_lookups as (
        select *
        from
            {{ params.raw_database }}.{{ params.raw_schema }}.{{ params.raw_table }}
    ),
    active_leads_with_no_or_expired_lookup as (
        select *
        from
            active_leads
                full outer join reverse_lookups using(phone_number_e164)
        where
            _last_lookup is null
            or _last_lookup < dateadd('month', -{{ params.lookup_refresh_months }}, current_date())
    )

select * from active_leads_with_no_or_expired_lookup