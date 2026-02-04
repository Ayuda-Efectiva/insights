# Copyright (c) 2022, Frappe Technologies Pvt. Ltd. and Contributors
# See license.txt


import frappe

_creds = None


def before_migrate():
    global _creds
    if frappe.db.exists("Insights Data Source v3", "Site DB"):
        _creds = frappe.db.get_value(
            "Insights Data Source v3",
            "Site DB",
            [
                "host",
                "port",
                "username",
                "password",
                "database_name",
                "use_ssl",
                "enable_stored_procedure_execution",
            ],
            as_dict=True,
        )


def after_migrate():
    global _creds
    try:
        create_admin_team()
        if _creds:
            frappe.db.set_value("Insights Data Source v3", "Site DB", _creds)
    except Exception:
        frappe.log_error(title="Error in after_migrate")


def create_admin_team():
    if not frappe.db.exists("Insights Team", "Admin"):
        frappe.get_doc(
            {
                "doctype": "Insights Team",
                "team_name": "Admin",
                "team_members": [{"user": "Administrator"}],
            }
        ).insert(ignore_permissions=True)
