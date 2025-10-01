
from functools import wraps
from flask import session, redirect, url_for, flash, request, current_app

ROLE_IMPLIES = {
    "admin": {"admin", "bursar", "headteacher", "dos", "clerk", "teacher"},
}

def _normalize_roles(value):
    if not value:
        return set()
    if isinstance(value, (list, tuple, set)):
        items = value
    else:
        items = str(value).split(",")
    return {str(r).strip().lower() for r in items if str(r).strip()}

def require_role(*allowed_roles):
    allowed = _normalize_roles(allowed_roles)

    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            user_id = session.get("user_id")
            raw_role = session.get("role")
            if not user_id or raw_role is None:
                flash("Please sign in.", "warning")
                return redirect(url_for("login", next=request.full_path))

            user_roles = _normalize_roles(raw_role)

            expanded = set(user_roles)
            for r in list(user_roles):
                expanded |= ROLE_IMPLIES.get(r, set())

            if not (expanded & allowed):
                current_app.logger.warning(
                    f"Access denied. Allowed={allowed}, User roles={user_roles}, Expanded={expanded}"
                )
                flash("You don't have permission to access this page.", "danger")
                return redirect(url_for("dashboard"))

            return fn(*args, **kwargs)
        return wrapped
    return decorator
