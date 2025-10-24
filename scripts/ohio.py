import pandas as pd
from bs4 import BeautifulSoup
import requests
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

url = "https://farmoffice.osu.edu/farm-management/enterprise-budgets"

def transform_year(year):
    """
    Transform the date as 2022 to 2022/2023
    """
    try:
        year = int(year)
        return f"{year}/{year+1}"
    except ValueError:
        logger.error(f"Invalid year format: {year}")
        return year

def extract_link(url):
    """
    This function allows to get all links
    """
    try:
        page = requests.get(url)
        page.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch URL {url}: {e}")
        raise ValueError("The URL provided is not working")

    soup = BeautifulSoup(page.text, "html.parser")
    elements = soup.find_all("ul")
    links = []
    for element in elements:
        references = element.find_all("a")
        for ref in references:
            links.append([ref["href"], ref.text])
    return links

def select_crop_budget_link(url: str) -> list[tuple]:
    """
    This function allows to get the right link related to the crop budget.
    It splits the text associated to the href in [link, commodity, year]
    """
    links = extract_link(url)
    link_name = []
    for link in links:
        if "Production Budget" in link[1] and link[0].split(".")[-1].lower() == "xls":
            # Improved splitting: split on whitespace and filter out empty strings
            parts = [p for p in link[1].replace("Production Budget", "").split() if p]
            if len(parts) >= 2:
                commodity = " ".join(parts[:-1])
                year = parts[-1]
                link_name.append([
                    "https://farmoffice.osu.edu" + link[0],
                    commodity,
                    year
                ])
            else:
                logger.warning(f"Unexpected format in link text: {link[1]}")
    return link_name

def extract_Ohio2(url):
    """
    This function allows to get the data inside all xlsx file get.
    It concatenates all data inside a common table
    """
    link_name_year = select_crop_budget_link(url)
    logger.info(f"Found {len(link_name_year)} links to process")

    Data = pd.DataFrame()

    for link in link_name_year:
        try:
            url_link, commodity, year = link[0], link[1], link[2]
            logger.info(f"Processing: {commodity} {year}")
            df = pd.read_excel(
                url_link, sheet_name="Quick Stats", usecols="A,G,I", nrows=25, skiprows=2
            ).dropna()
            df.rename(
                columns={
                    df.columns[0]: "Item",
                    df.columns[1]: "Low",
                    df.columns[2]: "High",
                },
                inplace=True,
            )
            price_df = pd.read_excel(
                url_link, sheet_name="Quick Stats", usecols="D", nrows=25, skiprows=2
            ).dropna()
            if not price_df.empty:
                price = price_df.values[0][0]
                Price = pd.DataFrame({"Item": ["Price"], "Low": [price], "High": [price]})
                df = pd.concat([df, Price], ignore_index=True)
            else:
                logger.warning(f"No price data found for {commodity} {year}")
                continue

            DF = (
                pd.melt(
                    df,
                    id_vars=["Item"],
                    value_vars=["Low", "High"],
                    ignore_index=False,
                )
                .reset_index()
                .drop("index", axis=1)
            )

            Unit = []
            for item in DF["Item"]:
                if item == "Price":
                    Unit.append("$/bushel")
                elif item == "Receipts":
                    Unit.append("bu/acre")
                else:
                    Unit.append("$/acre")

            DF = DF.assign(
                Location="Ohio",
                Source="Farmoffice",
                Commodity=commodity,
                Year=year,
                Unit=Unit,
            )

            Data = pd.concat([Data, DF])

        except Exception as e:
            logger.error(f"Error processing {link}: {e}")
            continue

    Data["Year"] = Data["Year"].apply(transform_year)
    Data = Data.rename(columns={"variable": "Soil type"})
    return Data

if __name__ == "__main__":
    DF = extract_Ohio2(url)
    DF.to_csv("historicals_ohio_2009_2011.csv", index=False)
    logger.info("Data extraction complete. CSV saved.")
