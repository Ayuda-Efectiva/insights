# Copyright (c) 2026, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class InsightsLockLog(Document):
    pass


def create_lock_log(event_type: str, lock_key: str = None, query_name: str = None, semaphore_count: int = None):
    """
    Create a log entry for lock events.
    """
    try:
        doc = frappe.get_doc({
            "doctype": "Insights Lock Log",
            "event_type": event_type,
            "lock_key": lock_key[:140] if lock_key else None,  # Truncate to fit Data field
            "query": query_name,
            "semaphore_count": semaphore_count,
            "user": frappe.session.user,
        })
        doc.insert(ignore_permissions=True)
    except Exception:
        # Don't let logging failures affect query execution
        pass
