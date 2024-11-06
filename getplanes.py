import argparse
import collections
import datetime
import itertools
import os
import pickle
import time
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from typing import List, Union

import gspread
import pandas as pd
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait

# available leases: _ctl0_MainContent_dtgAvailableLease
# selection list: selection_list.select_by_visible_text('')


class LeaseStrainer:
    def __init__(self, dom) -> None:
        self.soup = BeautifulSoup(dom)

    def get_table(self) -> pd.DataFrame:
        table = self.soup.find("table")
        df = pd.read_html(StringIO(str(table)), header=0)[0]
        df.drop(df.tail(1).index, inplace=True)
        return df


class BrowserAgent:
    def __init__(self) -> None:
        self.driver = self._get_webdriver()
        self.url = "https://env.airlineonline.aero/RockyAOLive"
        # We pre-initialize a Wait object for our webdriver
        self.wait = WebDriverWait(self.driver, 10)
        self.username = "mpp"
        self.password = "aerluxe"

    def _get_webdriver(self) -> webdriver.Chrome:
        # Define Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")

        # Define paths
        user_home_dir = os.path.expanduser("~")
        chrome_binary_path = os.path.join(
            user_home_dir, "aeromate", "chrome-linux64", "chrome"
        )
        chromedriver_path = os.path.join(
            user_home_dir, "aeromate", "chromedriver-linux64", "chromedriver"
        )

        # Set binary location and service
        chrome_options.binary_location = chrome_binary_path
        service = Service(chromedriver_path)

        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.implicitly_wait(2)
        return driver

    # Take the necesscary actions to log into the simulation
    # Resets the driver session to the authentication page
    # End state: Splash screen
    def _authenticate_airsim(self) -> None:
        self.driver.get(self.url)

        # Log in
        username_field = self.driver.find_element(By.ID, "txtUsername")
        password_field = self.driver.find_element(By.ID, "txtPassword")

        username_field.send_keys(self.username)
        password_field.send_keys(self.password)

        self.driver.find_element(By.NAME, "btnLogin").click()

        self.wait.until(EC.visibility_of_element_located((By.ID, "btnStartSimulation")))

    # Take the necesscary actions to bypass the splash page
    # Must be called after authenticate_airsim
    # End state: Simulation home-screen, in new window context
    def _trigger_popup(self) -> None:
        # Trigger popup
        self.driver.find_element(By.NAME, "btnStartSimulation").click()

        # Get the current window handle
        parentWindow = self.driver.current_window_handle

        self.wait.until(EC.number_of_windows_to_be(2))

        # Switch context to new window

        # Loop through until we find a new window handle
        for window_handle in self.driver.window_handles:
            if window_handle != parentWindow:
                self.driver.switch_to.window(window_handle)
                break

        # Wait for the new tab to finish loading content
        self.wait.until(EC.title_is("AIRLINEOnline - RockyAOLive, AeroLuxe"))

    # Take the necesscary actions to navigate to the Used Leases page.
    # Must be called after trigger_popup
    # End state: Available leases page
    def goto_leases(self) -> None:
        # Navigate to Aircrafts page
        link = self.driver.find_element(By.LINK_TEXT, "Aircraft").click()

        # Navigate to Leased Use Aircraft
        used_aircraft = self.driver.find_element(
            By.ID, "_ctl0_MainContent_btnLeaseUsedAircrafts"
        ).click()

    # Login to the simulation and navigate to the used-leases page.
    # Handles the stateful dependency of WebDriver by calling
    # helper methods in correct order.
    def login_workflow(self) -> None:
        self._authenticate_airsim()
        self._trigger_popup()
        self.goto_leases()

    # Get just the first page of leases for a given airframe (for speed and profit)
    def get_lease_page(self, airframe: str) -> Union[pd.DataFrame, None]:
        # Select the desired airframe
        selection_list = Select(
            self.driver.find_element(By.ID, "_ctl0_MainContent_ddlBudgetLease")
        )
        # Returns a list of (option_id, name) pairs for every available lease type
        available_airframes = {
            x.text: x.get_attribute("value") for x in selection_list.options
        }
        if airframe not in available_airframes.keys():
            print(f"DEBUG: Airframe of type {airframe} not currently found...")
            return None
        selection_list.select_by_value(available_airframes[airframe])  # type: ignore
        # Filter by selection
        apply_filter = self.driver.find_element(
            By.ID, "_ctl0_MainContent_btnApplyFilter"
        ).click()
        # Delay until the page updates
        self.wait.until(
            EC.visibility_of_element_located(
                (By.ID, "_ctl0_MainContent_dtgAvailableLease")
            )
        )
        # Pass the DOM to bs4, extract table.
        return LeaseStrainer(self.driver.page_source).get_table()

    # Get all available leases for a given airframe
    # If there are no available leases, return empty list
    def get_leases(self, airframe: str) -> List[pd.DataFrame]:
        # Select the desired airframe
        selection_list = Select(
            self.driver.find_element(By.ID, "_ctl0_MainContent_ddlBudgetLease")
        )
        # Returns a list of (option_id, name) pairs for every available lease type
        available_airframes = {
            x.text: x.get_attribute("value") for x in selection_list.options
        }
        if airframe not in available_airframes.keys():
            print(f"DEBUG: Airframe of type {airframe} not currently found...")
            return None
        selection_list.select_by_value(available_airframes[airframe])  # type: ignore
        # Filter by selection
        apply_filter = self.driver.find_element(
            By.ID, "_ctl0_MainContent_btnApplyFilter"
        ).click()
        # Delay until the page updates
        self.wait.until(
            EC.visibility_of_element_located(
                (By.ID, "_ctl0_MainContent_dtgAvailableLease")
            )
        )
        # Pass the DOM to bs4, extract table.
        tables = []
        tables.append(LeaseStrainer(self.driver.page_source).get_table())

        # get total number of pages
        navbar = self._get_navbar()
        pages = navbar.find_elements(By.TAG_NAME, "a")

        for idx in range(len(pages)):
            # Update the table
            print(f"DEBUG: {idx}")
            # Get the page from the navbar
            navbar = self._get_navbar()
            page = navbar.find_elements(By.TAG_NAME, "a")[idx]
            page.click()
            # Wait for the table to update and render
            self.wait.until(
                EC.visibility_of_element_located(
                    (By.ID, "_ctl0_MainContent_dtgAvailableLease")
                )
            )
            # Append the new table information
            tables.append(LeaseStrainer(self.driver.page_source).get_table())
        return tables

    def _get_navbar(self) -> WebElement:
        # Find element assosciated with table
        table = self.driver.find_element(By.TAG_NAME, "table")
        # Return the last row of this table.
        return table.find_element(By.XPATH, "//tr[last()]")

    # From any page, jump to the specified next page.
    def _goto_page(self, pagenum: int) -> None:
        navbar = self._get_navbar()
        # link = navbar.find_element(By.XPATH, f"//td[{pagenum}]")
        links = navbar.find_elements(By.TAG_NAME, "a")
        if not links:
            return None
        link = links[pagenum - 1]
        link.click()
        # wait until page updates
        self.wait.until(
            EC.visibility_of_element_located(
                (By.ID, "_ctl0_MainContent_dtgAvailableLease")
            )
        )

    # Move the webdriver to the leasing page.
    def purchase_aircraft(self, pagenum: int, rownum: int) -> None:
        # First, navigate to the correct page.
        # WARN: If pagenum is a negative number, assumes we are already on the right page.
        if pagenum >= 0:
            self._goto_page(pagenum)
        # Get the corresponding row from the table
        table = self.driver.find_element(By.TAG_NAME, "table")
        # row = table.find_element(By.XPATH, f"//tr[{rownum}]")
        # Get the link from this row
        link = table.find_elements(By.TAG_NAME, "a")[rownum - 1]
        # link = row.find_element(By.TAG_NAME, "a")
        link.click()
        # Wait until the page updates
        self.wait.until(
            EC.visibility_of_element_located((By.ID, "_ctl0_MainContent_btnGetQuote"))
        )
        # DEBUG: Get info table and print it
        table = self.driver.find_element(By.CLASS_NAME, "table.panel-content")
        print(table.text)

        # Select pre-pay option
        prepay = self.driver.find_element(By.ID, "_ctl0_MainContent_chkPrepay").click()

        # Purchase the airframe
        self.driver.find_element(By.ID, "_ctl0_MainContent_btnGetQuote").click()
        self.wait.until(
            EC.visibility_of_element_located((By.ID, "_ctl0_MainContent_btnAccept"))
        )
        # self.driver.find_element(By.ID, "_ctl0_MainContent_btnAccept").click()
        self.goto_leases()


class SheetsHandler:
    def __init__(self) -> None:
        self.creds = "aeroluk-f098d12de291.json"
        self.scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        self.sheetname = "AeroLuxe Flight Lease Monitoring"
        self.client = self._getGoogleAuth()

    def _getGoogleAuth(self) -> gspread.client.Client:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.creds, self.scope  # type: ignore
        )
        return gspread.authorize(credentials)

    def get_spreadsheet(self) -> pd.DataFrame:
        spreadsheet = self.client.open(self.sheetname).sheet1
        data = spreadsheet.get_all_values()
        headers = data.pop(0)
        df = pd.DataFrame(data, columns=headers)  # type: ignore
        return df


def build_table_index(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    newindex = list(
        itertools.chain.from_iterable(
            [itertools.repeat(i, len(x)) for i, x in enumerate(dfs)]
        )
    )

    # first, collate all of the list of tables into a multi-index dataframe
    df = pd.concat(dfs)
    df["pagenum"] = newindex
    df.set_index(["pagenum", df.index], inplace=True)
    df["Hours flown"] = df["Hours flown"].astype(int)
    return df


def main():

    if os.path.isfile("count.pickle"):
        with open("count.pickle", "rb") as handle:
            airframe_count = pickle.load(handle)
    else:
        airframe_count = collections.defaultdict(int)
    # TODO: handle selenium crashes
    while True:
        try:
            # Get a new Chrome Driver instance
            driver = BrowserAgent()
            # Navigate to the used leases page
            driver.login_workflow()
            # Create new timer (refresh user agent every 15 minutes)
            refresh_time = datetime.datetime.now() + datetime.timedelta(minutes=15)
            while datetime.datetime.now() < refresh_time:
                # Get the desired leases spreadsheet
                gsheets = SheetsHandler()
                desired_leases = gsheets.get_spreadsheet()
                # Create new timer (refresh spreadsheet every 3 minutes)
                sheets_refresh_time = datetime.datetime.now() + datetime.timedelta(
                    minutes=3
                )
                while datetime.datetime.now() < sheets_refresh_time:
                    for i, row in desired_leases.iterrows():
                        # Every time we load a new aircraft, we should refresh the leases page
                        driver.goto_leases()
                        # Get the available leases for this aircraft type
                        tables = driver.get_leases(row["Aircraft Type"])
                        if not tables:
                            continue
                        df = build_table_index(tables)
                        while airframe_count[row["Aircraft Type"]] < int(
                            row["Maximum Airframes"]
                        ):
                            # Filter out any values above maximum flight hours
                            df = df[df["Hours flown"] < int(row["Maximum Hours"])]
                            # If there are no remaining valid aircraft, exit
                            if df.shape[0] <= 0:
                                break
                            # Sort dataframe by flight hours
                            df = df.sort_values("Hours flown")
                            # For all airframes below the maximum flight hours, purchase the lease
                            for j, lease in df.iterrows():
                                driver.purchase_aircraft(j[0] + 1, j[1] + 1)

                                # Every time we purchase an aircraft, update the airframe count and store it to disk
                                # This is i/o intensive and inefficient, but we are okay with slowing down this thread
                                airframe_count[row["Aircraft Type"]] += 1
                                with open("filename.pickle", "wb") as handle:
                                    pickle.dump(
                                        airframe_count,
                                        handle,
                                        protocol=pickle.HIGHEST_PROTOCOL,
                                    )
                    time.sleep(2)
        # In the event selenium crashes, spin up a new user agent and try again
        except Exception as e:
            print(f"ERROR: {e}")
            continue


def launch_agent(aircraft_type: str, max_airframes: int) -> None:
    # Create a new web driver
    purchases = 0
    while purchases < max_airframes:
        try:
            driver = BrowserAgent()
            driver.login_workflow()
            while purchases < max_airframes:
                start_time = time.time()
                try:
                    driver.goto_leases()
                    tables = driver.get_lease_page(aircraft_type)
                    if not type(tables) == pd.DataFrame:
                        continue
                    else:
                        driver.purchase_aircraft(-1, 1)
                        purchases += 1
                    print(f"INFO: Time to purchase: {time.time() - start_time}")
                except Exception as e:
                    print(
                        f"WARN: Purchasing airframe {aircraft_type} encountered error {e}"
                    )
                    continue
        except Exception as e:
            print(f"WARN: Failed to login, retrying...")
            continue


def saturation_attack() -> None:
    # Load the spreadsheet
    gsheets = SheetsHandler()
    desired_leases = gsheets.get_spreadsheet()
    # open multithreading context manager
    with ThreadPoolExecutor(max_workers=10) as exe:
        # For every airframe type, create a new user agent.
        for _, row in desired_leases.iterrows():
            exe.submit(
                launch_agent, row["Aircraft Type"], int(row["Maximum Airframes"])
            )


def parseargs() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        type=str,
        help="Switch bot between monitor and saturation modes.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parseargs()
    # Inlcude a switch for special batch run purchase mode
    if args.mode == "saturation":
        saturation_attack()

    else:
        main()
