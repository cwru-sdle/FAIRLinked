#!/usr/bin/env python3
"""
download_mds_ontology.py

Automates the download of MDS-Onto files from the SDLE public website using Selenium.

Functionality:
- Clicks the main `.ttl` download button
- Opens the dropdown to download `.jsonld`, `.nt`, and `.owl` formats
- Waits for each download to complete and renames the files
- Optionally captures screenshots for debugging if files are missing

Functions:
- download_mds_ontology(download_dir): Runs the entire download sequence
"""

import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def wait_for_download_and_rename(download_dir, extension, label):
    """
    Waits for a new file with the given extension to appear in the download directory.
    Once detected, renames it with a timestamp and a user-friendly label.

    Args:
        download_dir (str): Directory where downloads are saved.
        extension (str): File extension to look for (e.g., '.ttl', '.jsonld').
        label (str): Short label to prefix the renamed file (e.g., 'ttl', 'jsonld').
    """
    print(f"⏳ Waiting for {extension} file...")
    before = set(os.listdir(download_dir))
    for _ in range(30):
        time.sleep(1)
        after = set(os.listdir(download_dir))
        new_files = after - before
        target_files = [f for f in new_files if f.endswith(extension)]
        if target_files:
            filename = target_files[0]
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            new_name = f"MDS_Onto-{label}-{timestamp}{extension}"
            os.rename(
                os.path.join(download_dir, filename),
                os.path.join(download_dir, new_name)
            )
            print(f"✅ Downloaded and renamed to: {new_name}")
            return
    print(f"❌ Timeout: {extension} file not found.")

def download_mds_ontology(download_dir):
    """
    Automates downloading of MDS-Onto `.ttl`, `.jsonld`, `.nt`, and `.owl` files.

    Args:
        download_dir (str): Directory to save the downloaded files.
    """
    os.makedirs(download_dir, exist_ok=True)

    options = Options()
    # options.add_argument("--headless")  # Uncomment for headless operation
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=options)
    dropdown_xpath = "//h1[contains(text(), 'FindTheDocs')]/following::button[@aria-label='dropdown menu'][1]"

    try:
        driver.get("https://cwrusdle.bitbucket.io/")

        # Download .ttl
        ttl_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Download .ttl file')]"))
        )
        ttl_button.click()
        wait_for_download_and_rename(download_dir, ".ttl", "ttl")

        # Open dropdown
        dropdown_arrow = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, dropdown_xpath))
        )
        dropdown_arrow.click()
        print("📂 Opened download dropdown")

        file_options = {
            "jsonld": ("Download JSON-LD file", ".jsonld"),
            "nt": ("Download .nt file", ".nt"),
            "rdfowl": ("Download RDF/OWL file", ".owl")
        }

        for label, (visible_text, extension) in file_options.items():
            # Wait for menu items
            elements = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.XPATH, "//li[@role='menuitem']"))
            )
            print("📋 Found menu items:", [e.text.strip() for e in elements])

            clicked = False
            for el in elements:
                if el.text.strip() == visible_text:
                    el.click()
                    clicked = True
                    break

            if not clicked:
                print(f"❌ Could not find menu item: '{visible_text}'")
                continue

            wait_for_download_and_rename(download_dir, extension, label)

            # Reopen dropdown for next file
            time.sleep(0.5)
            dropdown_arrow = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, dropdown_xpath))
            )
            dropdown_arrow.click()

    finally:
        driver.quit()
