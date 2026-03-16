import re

# --------------------------------------------------
# Funnel role inference (internal logic only)
# --------------------------------------------------

EXPOSURE_REGEX   = re.compile(r"(impression|reach|grp|trp|imp|spot|sent|deliver)", re.IGNORECASE)
ENGAGEMENT_REGEX = re.compile(r"(click|view|visit|engage|clk|read)", re.IGNORECASE)
CONVERSION_REGEX = re.compile(r"(conversion|conv|order|install|purchase)", re.IGNORECASE)


def infer_funnel_role(field_name: str, data_category: str, data_type: str) -> str | None:
    """
    Infer funnel role from existing metadata + name heuristics.
    Returns one of:
    exposure, engagement, conversion, cost, efficiency
    """

    if data_category.lower() == "spend":
        return "cost"

    if data_category.lower() == "response":
        if data_type.lower() == "rate":
            return "efficiency"

        if EXPOSURE_REGEX.search(field_name):
            return "exposure"

        if ENGAGEMENT_REGEX.search(field_name):
            return "engagement"

        if CONVERSION_REGEX.search(field_name):
            return "conversion"

    return None