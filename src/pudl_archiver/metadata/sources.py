"""Metadata and operational constants."""

from typing import Any

import pandas as pd
from pudl.metadata.constants import CONTRIBUTORS, LICENSES

# To add a new contributor, follow the following format to add an entry to the
# ADDL_CONTRIBUTORS dictionary below formatted like this:
#     "name-shorthand": {
#         "title": "Catalyst Cooperative",
#         "email": "pudl@catalyst.coop",
#         "path": "https://catalyst.coop",
#         "role": "publisher",
#         "zenodo_role": "distributor",
#         "organization": "Catalyst Cooperative",
#         "orcid": "0000-1234-5678-9101"
#     }
# Note that the only required fields are title (your name) and path
# (e.g., a link to your Github account, your ORCID site or a personal webpage), but
# filling other fields is strongly encouraged!
ADDL_CONTRIBUTORS: dict[str, dict[str, str]] = {}

NON_PUDL_SOURCES: dict[str, Any] = {
    "doelead": {
        "title": "DOE LEAD -- Low Income Energy Affordability Data",
        "path": "https://www.energy.gov/scep/low-income-energy-affordability-data-lead-tool",
        "description": (
            "This archive includes the data behind the Department of Energy's (DOE)"
            "Low Income Energy Affordability Data (LEAD) tool. The LEAD tool is an"
            "online, interactive platform that helps users make data-driven decisions"
            "on energy goals and program planning by improving their understanding of"
            "low-income and moderate-income household energy characteristics. The LEAD"
            "Tool offers the ability to select and combine geographic areas (state,"
            "county, city and census tract) into one customized group so users can see"
            "the total area for their customized geographies (e.g., specific service"
            "territories)."
        ),
        "working_partitions": {"years": [2022, 2018]},
        "keywords": sorted(
            {"doe", "lead", "low income", "energy affordability", "energy burden"}
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
    },
    "eiacbecs": {
        "title": "EIA CBECS -- Commercial Buildings Energy Consumption Survey",
        "path": "https://www.eia.gov/consumption/manufacturing/data/2018/",
        "description": (
            "The Commercial Buildings Energy Consumption Survey (CBECS) is a national"
            "sample survey that collects information on the stock of U.S. commercial"
            "buildings, including their energy-related building characteristics and"
            "energy usage data (consumption and expenditures). Commercial buildings"
            "include all buildings in which at least half of the floorspace is used for"
            "a purpose that is not residential, industrial, or agricultural. By this"
            "definition, CBECS includes building types that might not traditionally be"
            "considered commercial, such as schools, hospitals, correctional"
            "institutions, and buildings used for religious worship, in addition to"
            "traditional commercial buildings such as stores, restaurants, warehouses,"
            "and office buildings."
        ),
        "working_partitions": {
            "years": [
                2003,
                2012,
                2018,
            ]  # there are PDF only versions for 1999, 1995 and 1992
        },
        "keywords": sorted(
            {"eia", "energy", "cbecs", "consumption", "buildings", "commercial"}
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
    },
    "eiamecs": {
        "title": "EIA MECS -- Manufacturing Energy Consumption Survey",
        "path": "https://www.eia.gov/consumption/manufacturing/data/2018/",
        "description": (
            "EIA Form 846 A and B is more commonly known as the Manufacturing Energy"
            "Consumption Survey (MECS). MECS is a national sample survey that collects"
            "information on the stock of U.S. manufacturing establishment, their"
            "energy-related building characteristics, and their energy consumption"
            "and expenditures. MECS is conducted every four years."
        ),
        "working_partitions": {
            "years": [1991, 1994, 1998, 2002, 2006, 2010, 2014, 2018]
        },
        "keywords": sorted(
            {
                "manufacturing",
                "MECS",
            }
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
    },
    "eiarecs": {
        "title": "EIA RECS -- Residential Energy Consumption Survey",
        "path": "https://www.eia.gov/consumption/residential/data/2020/",
        "description": (
            "EIA administers the Residential Energy Consumption Survey (RECS) to a"
            "nationally representative sample of housing units. Traditionally,"
            "specially trained interviewers collect energy characteristics on the"
            "housing unit, usage patterns, and household demographics. For the 2020"
            "survey cycle, EIA used Web and mail forms to collect detailed information"
            "on household energy characteristics. This information is combined with"
            "data from energy suppliers to these homes to estimate energy costs and"
            "usage for heating, cooling, appliances and other end uses — information"
            "critical to meeting future energy demand and improving efficiency and"
            "building design. "
        ),
        "working_partitions": {
            "years": [2009, 2015, 2020]  # Only PDFs: 1993, 1997, 2001, 2005
        },
        "keywords": sorted(
            {
                "manufacturing",
                "MECS",
            }
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
    },
    "usgsuswtdb": {
        "title": "USGS USWTDB - U.S. Wind Turbine Database",
        "path": "https://energy.usgs.gov/uswtdb/data/",
        "description": (
            "The United States Wind Turbine Database (USWTDB) provides the locations"
            "of land-based and offshore wind turbines in the United States,"
            "corresponding wind project information, and turbine technical"
            "specifications. Wind turbine records are collected and compiled from"
            "various public and private sources, digitized and position-verified from"
            "aerial imagery, and quality checked. The USWTDB is available for download"
            "in a variety of tabular and geospatial file formats, to meet a range of"
            "user/software needs. Dynamic web services are available for users that wish"
            "to access the USWTDB as a Representational State Transfer Services (RESTful)"
            "web service."
        ),
        "working_partitions": {
            "year_quarters": [
                str(q).lower()
                for q in pd.period_range(start="2018q2", end="2024q2", freq="Q")
            ]  # Note: this looks mostly right but not always. maybe this should be year_month
            # and we should just enumerate the months.
        },
        "keywords": sorted(
            {
                "usgs",
                "uswtdb",
                "wind",
                "wind turbines",
            }
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
    },
    "usgsuspvdb": {
        "title": "USGS USPVDB -- U.S. Large-Scale Solar Photovoltaic Database",
        "path": "https://energy.usgs.gov/uspvdb/",
        "description": (
            "The United States Large-Scale Solar Photovoltaic Database (USPVDB) provides"
            "the locations and array boundaries of U.S. ground-mounted photovoltaic (PV)"
            "facilities with capacity of 1 megawatt or more. It includes corresponding PV"
            "facility information, including panel type, site type, and initial year of"
            "operation. The creation of this database was jointly funded by the U.S."
            "Department of Energy (DOE) Solar Energy Technologies Office (SETO) via the"
            "Lawrence Berkeley National Laboratory (LBNL) Energy Markets and Policy"
            "Department, and the U.S. Geological Survey (USGS) Energy Resources Program."
            "The PV facility records are collected from the U.S. Energy Information"
            "Administration (EIA), position-verified and digitized from aerial imagery,"
            "and checked for quality. EIA facility data are supplemented with additional"
            "attributes obtained from public sources."
        ),
        "working_partitions": {"years": [2023, 2024]},
        "keywords": sorted(
            {
                "usgs",
                "uspvdb",
                "solar",
                "pv",
                "photovoltaic",
            }
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
    },
    "epaegrid": {
        "title": "EPA eGRID -- Emissions & Generation Resource Integrated Database",
        "path": "https://www.epa.gov/egrid",
        "description": (
            "The Emissions & Generation Resource Integrated Database (eGRID) is a"
            "comprehensive source of data from EPA's Clean Air Power Sector Programs on"
            "the environmental characteristics of almost all electric power generated in"
            "the United States. The data includes emissions, emission rates, generation,"
            "heat input, resource mix, and many other attributes. eGRID is typically used"
            "for greenhouse gas registries and inventories, carbon footprints for"
            "electricity purchases, consumer information disclosure, emission inventories"
            "and standards, power market changes, and avoided emission estimates."
        ),
        "working_partitions": {"years": [2018, 2019, 2020, 2021, 2022, 2023]},
        "keywords": sorted(
            {
                "epa",
                "egrid",
                "emissions",
                "greenhouse gas",
                "heat input",
                "resource mix",
                "carbon footprint",
                "avoided emissions",
            }
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
    },
}
