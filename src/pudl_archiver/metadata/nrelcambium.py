"""NREL Cambium -specific metadata helper."""

from pudl.metadata.constants import CONTRIBUTORS, KEYWORDS, LICENSES


def nrel_cambium_generator(year):
    """Generate metadata dictionaries for NREL Cambium.

    NREL Cambium datasets are too large to group together under a "years" partition, but
    otherwise share metadata.
    """
    return {
        "title": f"NREL Cambium {year}",
        "path": "https://www.nrel.gov/analysis/cambium.html",
        "description": (
            f"""Cambium datasets contain modeled hourly data for a range of possible futures of the U.S. electricity sector, with metrics designed to be useful for forward-looking analysis and decision support.\n\n

Cambium is annually updated and expands on the metrics reported in NREL’s Standard Scenarios—another annually released set of projections of how the U.S. electric sector could evolve across a suite of potential futures.\n\n

The {year} Cambium release includes two products:\n\n

(1) The full {year} Cambium datasets;\n
(2) NREL reports describing the scenarios, defining metrics and methods, describing major changes since the last release, and discussing intended uses and limitations of the dataset."""
        ),
        "source_file_dict": {
            "source_format": "CSV",
        },
        "working_partitions": {
            "scenarios": [
                "all",
                "high_demand_growth",
                "high_natural_gas_prices",
                "high_renewable_energy_cost",
                "low_natural_gas_prices",
                "low_renewable_energy_cost",
                "mid_case",
                "mid_case_with_100pct_decarbonization_by_2035",
                "mid_case_with_95pct_decarbonization_by_2050",
                "high_electrification",
                "low_renewable_energy_cost_with_tax_credit_expiration",
                "mid_case_with_tax_credit_expiration",
                "high_renewable_energy_cost",
                "low_renewable_energy_cost",
                "mid_case_95_by_2035",
                "mid_case_95_by_2050",
            ]
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
        ],
        "keywords": sorted(
            set(
                [
                    "nrel",
                    "cambium",
                ]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
            )
        ),
        "license_raw": LICENSES["cc-by-4.0"],
        "license_pudl": LICENSES["cc-by-4.0"],
    }
