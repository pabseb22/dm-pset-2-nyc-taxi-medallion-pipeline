from mage_ai.orchestration.run_status_checker import check_status

if 'sensor' not in globals():
    from mage_ai.data_preparation.decorators import sensor


@sensor
def check_condition(*args, **kwargs) -> bool:
    execution_date = kwargs.get('execution_date')
    if not execution_date:
        return False

    silver_ok = check_status(
        'dbt_build_silver',
        execution_date,
        hours=24,
    )

    gold_already_ran = check_status(
        'db_build_gold',   # reemplaza por el UUID real de tu silver
        execution_date,
        hours=24,
    )

    return silver_ok and not gold_already_ran