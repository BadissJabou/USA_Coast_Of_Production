#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
from pypdf import PdfReader  # Updated from PyPDF2
import tempfile
import os
from io import StringIO
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_year(year):
    """
    Transform the date as 2022 to 2022/2023
    """
    year = int(year)
    return f"{year}/{year+1}"

def strip_characters(input_string, characters_to_remove):
    translation_table = str.maketrans("", "", characters_to_remove)
    stripped_string = input_string.translate(translation_table)
    return stripped_string

def last_uppercase_position(text):
    match = re.search(r'[A-Z]', text[::-1])
    if match:
        position = len(text) - match.start() - 1
        return position
    else:
        return None

def extract_yield_from_title(title):
    """
    Extract yield value from title.
    """
    if "bu" in title and "yie" in title:
        value = title.split('bu')[0].split(',')[-1]
        if '"' in value:
            value = value.split('"')[-1].strip()
        else:
            value = value.split(' ')[-1].strip()
        return value
    return -1

def extract_product_from_title(title):
    """
    Extract product from title.
    """
    if not('acre ' in title):
        title = title.replace('acre', 'acre ')
    if title.startswith("---------"):
        product = title.split('acre ')[1].split(',')[0]
    else:
        product = title.split('acre ')[1].split(',')[0]
    return product

def process_pdf_batch(pdf_links, year_from_url=False):
    """
    Process a batch of PDF links and extract data into a DataFrame.
    
    Args:
        pdf_links (list): List of PDF URLs.
        year_from_url (bool): If True, extract year from URL; else from title.
    
    Returns:
        pd.DataFrame: Extracted data.
    """
    DATA = pd.DataFrame([])
    for pdf_link in pdf_links:
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
                response = requests.get(pdf_link, timeout=10)
                response.raise_for_status()
                temp_file.write(response.content)
                temp_file_path = temp_file.name
            pdf_reader = PdfReader(temp_file_path)
            logger.info(f'Read! {pdf_link}')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading {pdf_link}: {e}")
            continue
        except Exception as e:
            logger.error(f"Error processing {pdf_link}: {e}")
            continue
        finally:
            if 'temp_file_path' in locals():
                try:
                    os.unlink(temp_file_path)
                except:
                    pass

        for page in pdf_reader.pages:
            text = page.extract_text().strip()
            text = strip_characters(text, "_")
            if len(text) > 30:
                init_ = text.replace(' ', '')
                if ('Tab' in init_) and ('BSum' in init_):
                    final = init_
                    clean = final.split('ITEM')[0].replace(' ', '').strip().split("\n")
                    title = ' '.join(clean)

                    # Extract year
                    if year_from_url:
                        year = "20" + pdf_link.split('docs/')[-1].split('/')[0]
                    else:
                        if title.startswith("---------"):
                            year = title.replace(' ', '').replace('_', '')[-4:]
                        else:
                            year = title[-4:]

                    # Extract yield
                    yield_value = extract_yield_from_title(title)
                    yield_ = ['Yield', 'bu', yield_value]

                    # Extract product
                    product = extract_product_from_title(title)

                    irrigation = "Irrigated" if "Irrig" in title else "Not available"
                    tilled = "Tilled" if "till" in title else "Not available"

                    try:
                        table = final.split('DIRECTEXPENSES\n')[1]
                    except IndexError:
                        try:
                            table = final.split('DIRECTEXPENSES')[1]
                        except IndexError:
                            logger.warning(f"No DIRECTEXPENSES section in {pdf_link}")
                            continue

                    output = "ITEM\tUNIT\tAMOUNT\n"
                    lines = []
                    for line in table.split('\n'):
                        if len(line) < 40:
                            lines.append(line)
                        else:
                            nested_list = re.sub(r'(\d)(?=[A-Z])', r'\1\n', line)
                            lines += nested_list.split("\n")

                    for line in lines:
                        if "RETURN" in line:
                            break
                        if not('TOTAL' in line):
                            pos = last_uppercase_position(line)
                            if pos is None:
                                continue
                            item = line[:pos+1]
                            line = line.replace('gal', 'gal ') if "gal" in line else line
                            unit = line[pos+1:pos+5] if line.count('.') <= 3 else "acre"
                            amount = line[line.rfind('.', 0, line.rfind('.') - 1)+5:]
                            output += f"{item}\t{unit}\t{amount}\n"

                    df = pd.read_csv(StringIO(output), sep='\t')
                    df.loc[len(df)] = yield_
                    df = df.assign(Year=year, Product=product, Location='Mississippi', Currency="USD", Irrigation=irrigation, Tilled=tilled)
                    DATA = pd.concat([DATA, df], ignore_index=True)
    
    # Clean data
    if not DATA.empty:
        DATA['AMOUNT'] = DATA['AMOUNT'].astype(str).str.replace(' ', '').str.replace('-', '').astype(float)
        DATA['Year'] = DATA['Year'].astype(str).str.replace('ppi,', '2023').astype(int)
    return DATA

# Main script
url = "https://www.agecon.msstate.edu/whatwedo/budgets/archive.php"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

recent_years = [str(datetime.now().year)[-2:], str(datetime.now().year + 1)[-2:]]

a_tags = soup.find_all('a')
all_links = []
for tag in a_tags:
    if 'docs' in tag['href'] and 'pdf' in tag['href']:
        link = url.split('archive')[0] + tag['href']
        if "budgets//whatwedo" in link:
            link = '//'.join(link.split('//')[:2]) + link.split('budgets')[-1]
        all_links.append(link)

# Filter for recent years
recent_links = [link for link in all_links if link.split('docs/')[-1].split('/')[0] in recent_years]

# Process in batches
batch_size = 35
for i, start in enumerate(range(0, len(recent_links), batch_size)):
    batch = recent_links[start:start + batch_size]
    year_from_url = i > 0  # First batch uses title, others use URL
    data = process_pdf_batch(batch, year_from_url=year_from_url)
    if not data.empty:
        data.to_csv(f"part{i+1}.csv", index=False)
    logger.info(f"Processed part {i+1}, unique items: {data['ITEM'].unique() if not data.empty else 'None'}")
    logger.info(f"Unique years: {data['Year'].unique() if not data.empty else 'None'}")
    logger.info(f"Unique products: {data['Product'].unique() if not data.empty else 'None'}")

