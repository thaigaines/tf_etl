from excel_to_raw import export_excel
from load_lookup_tables import load_lookup, load_procedure
from load_plan import load_plan
from load_plan_benefits import load_plan_benefits

export_excel()

load_lookup("Carrier", "carrier", "Carrier Name", "name")
load_lookup("Network Type", "network_type", "Network Type", "name")
load_lookup("Plan Category", "plan_category", "Plan Category", "category")
load_lookup("Plan Type", "plan_type", "Plan Type", "name")
load_lookup("Service Type", "service_type", "Service Type", "type")
load_procedure()

load_plan()

load_plan_benefits()