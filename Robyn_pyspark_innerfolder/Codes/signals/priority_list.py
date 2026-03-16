# ==========================================================
# Exhaustive Channel Exposure Priority Map
# ==========================================================

CHANNEL_EXPOSURE_PRIORITY = {

    # ==================================================
    # CRM / DIRECT COMMUNICATION
    # ==================================================

    "email": [
        ["delivered", "delivery"],
        ["sent", "send"],
        ["open", "opens"],
        ["click", "clicks", "clk", "clks"]
    ],

    "sms": [
        ["delivered", "delivery"],
        ["sent", "send"],
        ["click", "clicks", "clk", "clks"]
    ],

    "whatsapp": [
        ["delivered", "delivery"],
        ["sent", "send"],
        ["read", "seen"],
        ["click", "clicks", "clk", "clks"]
    ],

    "push": [
        ["delivered", "delivery"],
        ["sent", "send"],
        ["open", "opens"],
        ["click", "clicks", "clk", "clks"]
    ],

    "inapp": [
        ["delivered", "shown", "impression"],
        ["click", "clicks", "clk", "clks"]
    ],

    "notification": [
        ["delivered", "shown"],
        ["open", "opens"]
    ],


    # ==================================================
    # TV / VIDEO / BROADCAST
    # ==================================================

    "tv": [
        ["grp", "trp"]
    ],

    "television": [
        ["grp", "trp"]
    ],

    "cable": [
        ["grp", "trp"]
    ],

    "satellite": [
        ["grp", "trp"]
    ],

    "ott": [
        ["view", "views", "complete", "completion"],
        ["impression", "impr"]
    ],

    "ctv": [
        ["view", "views"],
        ["impression"]
    ],

    "connectedtv": [
        ["view", "views"],
        ["impression"]
    ],

    "youtube": [
        ["view", "views"],
        ["impression"]
    ],


    # ==================================================
    # DISPLAY / PROGRAMMATIC / NATIVE
    # ==================================================

    "display": [
        ["impression", "impr", "imp"]
    ],

    "programmatic": [
        ["impression", "impr", "imp"]
    ],

    "native": [
        ["impression"]
    ],

    "banner": [
        ["impression"]
    ],

    "richmedia": [
        ["impression", "interaction"]
    ],

    "retargeting": [
        ["impression"],
        ["click", "clicks", "clk", "clks"]
    ],


    # ==================================================
    # SOCIAL MEDIA / COMMUNITY
    # ==================================================

    "social": [
        ["reach"],
        ["impression", "impr"],
        ["engagement", "engage"]
    ],

    "facebook": [
        ["reach"],
        ["impression"],
        ["engagement"]
    ],

    "instagram": [
        ["reach"],
        ["impression"],
        ["engagement"]
    ],

    "meta": [
        ["reach"],
        ["impression"],
        ["engagement"]
    ],

    "twitter": [
        ["reach"],
        ["impression"],
        ["engagement"]
    ],

    "x": [
        ["reach"],
        ["impression"]
    ],

    "linkedin": [
        ["reach"],
        ["impression"],
        ["click","clicks", "clk", "clks"]
    ],

    "snapchat": [
        ["reach"],
        ["impression"],
        ["view"]
    ],

    "pinterest": [
        ["reach"],
        ["impression"],
        ["click", "clk", "clks","clicks"]
    ],

    "reddit": [
        ["impression"],
        ["engagement"]
    ],


    # ==================================================
    # SEARCH / INTENT
    # ==================================================

    "search": [
        ["click", "clicks", "clk","clks"]
    ],

    "sem": [
        ["click", "clicks", "clk", "clks"]
    ],

    "ppc": [
        ["click", "clicks", "clk", "clks"]
    ],

    "googleads": [
        ["click", "clicks", "clk", "clks"]
    ],

    "bing": [
        ["click", "clicks", "clk", "clks"]
    ],

    "yahoo": [
        ["click", "clicks", "clk", "clks"]
    ],


    # ==================================================
    # E-COMMERCE / RETAIL MEDIA
    # ==================================================

    "ecom": [
        ["click", "clicks", "clk", "clks"],
        ["pdp", "product", "detail"]
    ],

    "ecommerce": [
        ["click", "clicks", "clk", "clks"],
        ["pdp"]
    ],

    "marketplace": [
        ["click", "clk", "clks","clicks"],
        ["pdp"]
    ],

    "amazon": [
        ["click", "clicks", "clk", "clks"],
        ["pdp", "detail_page", "dpv"]
    ],

    "flipkart": [
        ["click", "clicks", "clk", "clks"],
        ["pdp"]
    ],

    "myntra": [
        ["click", "clicks", "clk", "clks"],
        ["pdp"]
    ],

    "ajio": [
        ["click", "clicks", "clk", "clks"],
        ["pdp"]
    ],

    "meesho": [
        ["click", "clicks", "clk", "clks"],
        ["pdp"]
    ],

    "swiggy": [
        ["click", "clicks", "clk", "clks"],
        ["menu_view", "pdp"]
    ],

    "zomato": [
        ["click", "clicks", "clk", "clks"],
        ["menu_view"]
    ],

    "blinkit": [
        ["click", "clk", "clks", "clicks"],
        ["product_view"]
    ],

    "instamart": [
        ["click", "clk", "clks", "clicks"],
        ["product_view"]
    ],

    "bigbasket": [
        ["click", "clk", "clks", "clicks"],
        ["product_view"]
    ],


    # ==================================================
    # PRINT / PUBLISHING
    # ==================================================

    "print": [
        ["circulation"],
        ["reach"]
    ],

    "newspaper": [
        ["circulation"],
        ["reach"]
    ],

    "magazine": [
        ["circulation"],
        ["reach"]
    ],

    "journal": [
        ["circulation"],
        ["reach"]
    ],


    # ==================================================
    # RADIO / AUDIO
    # ==================================================

    "radio": [
        ["grp", "trp"]
    ],

    "fm": [
        ["grp", "trp"]
    ],

    "audio": [
        ["impression", "listen"]
    ],

    "spotify": [
        ["listen", "impression"]
    ],

    "gaana": [
        ["listen", "impression"]
    ],

    "jiosaavn": [
        ["listen", "impression"]
    ],


    # ==================================================
    # OOH / DOOH / TRANSIT
    # ==================================================

    "ooh": [
        ["reach"],
        ["footfall", "traffic"]
    ],

    "dooh": [
        ["reach"],
        ["footfall"]
    ],

    "outdoor": [
        ["reach"],
        ["footfall"]
    ],

    "hoarding": [
        ["reach"],
        ["traffic"]
    ],

    "metro": [
        ["footfall"],
        ["reach"]
    ],

    "airport": [
        ["footfall"],
        ["reach"]
    ],

    "mall": [
        ["footfall"],
        ["reach"]
    ],

    "transit": [
        ["footfall"],
        ["reach"]
    ],


    # ==================================================
    # INFLUENCER / AFFILIATE / PARTNERS
    # ==================================================

    "influencer": [
        ["view", "views"],
        ["engagement", "engage"],
        ["click", "clk", "clks", "clicks"]
    ],

    "creator": [
        ["view"],
        ["engagement"]
    ],

    "affiliate": [
        ["click", "clicks", "clk", "clks"],
        ["conversion"]
    ],

    "partner": [
        ["click", "clk", "clks", "clicks"],
        ["lead"]
    ],


    # ==================================================
    # EVENTS / FIELD / ACTIVATION
    # ==================================================

    "event": [
        ["footfall", "attendance"],
        ["lead"]
    ],

    "activation": [
        ["footfall"],
        ["sampling"]
    ],

    "roadshow": [
        ["footfall"],
        ["lead"]
    ],

    "expo": [
        ["attendance"],
        ["lead"]
    ],


    # ==================================================
    # TELESALES / CALL CENTER
    # ==================================================

    "callcenter": [
        ["connected", "answered"],
        ["call", "calls"],
        ["lead"]
    ],

    "telesales": [
        ["connected"],
        ["conversion"]
    ],

    "ivr": [
        ["connected"],
        ["call"]
    ]
}


DEFAULT_EXPOSURE_PRIORITY = [
    ["impression", "impr", "imp"],
    ["reach"],
    ["grp", "trp"],
    ["view", "views"],
    ["click", "clicks", "clk", "clks"]
]

CONTROL_KEYS = ["gqv","macro","cpi","inflation","price","promo_flag","promo_offer_flag","promo_perc"]


# priority_list.py


# ==================================================
# Channel-wise RESPONSE Priority
# ==================================================

CHANNEL_RESPONSE_PRIORITY = {

    # CRM
    "email": [
        ["conversion", "purchase", "order"],
        ["click", "clk", "clks", "clicks"],
        [ "open","read"]
    ],

    "sms": [
        ["conversion", "order"],
        ["click", "clk", "clks", "clicks"]
    ],

    "whatsapp": [
        ["conversion"],
        ["click", "clk", "clks", "clicks"],
        [ "open","read"]
    ],


    # TV / OOH / Print (no direct response usually)
    "tv": [],
    "radio": [],
    "print": [],
    "ooh": [],


    # Display / Social
    "display": [
        ["conversion"],
        ["engagement"],
        ["click", "clk", "clks", "clicks"]
    ],

    "social": [
        ["conversion"],
        ["engagement"],
        ["click", "clk", "clks", "clicks"]
    ],

    "facebook": [
        ["conversion"],
        ["engagement"],
        ["click", "clk", "clks", "clicks"]
    ],

    "instagram": [
        ["conversion"],
        ["engagement"],
        ["click", "clk", "clks", "clicks"]
    ],


    # Search
    "search": [
        ["conversion", "order"],
        ["engagement"],
        ["click", "clk", "clks", "clicks"]
    ],

    "sem": [
        ["conversion"],
        ["engagement"],
        ["click", "clk", "clks", "clicks"]
    ],


    # Ecom
    "amazon": [
        ["order", "sale"],
        ["pdp", "product_view"]
    ],

    "flipkart": [
        ["order"],
        ["pdp"]
    ],

    "ecommerce": [
        ["order","purchase"],
        ["pdp","Product Detail Page"],
        ["engagement"],
        ["click", "clk", "clks", "clicks"]
    ],


    # Influencer / Affiliate
    "influencer": [
        ["conversion"],
        ["engagement"],
        ["click", "clk", "clks", "clicks"]
    ],

    "affiliate": [
        ["conversion"],
        ["engagement"],
        ["click", "clk", "clks", "clicks"]
    ]
}


DEFAULT_RESPONSE_PRIORITY = [
    ["order", "conversion", "sale"],
    ["engagement"],
    ["click", "clk", "clks", "clicks"],
    ["impression"]
]
