-- 気象台コード一覧を取得

select
    meteorological_observatory_code
from `{{project_id}}.tenmado_setting.m_fetch_meteorologicalobservatory`
order by
    meteorological_observatory_code
;