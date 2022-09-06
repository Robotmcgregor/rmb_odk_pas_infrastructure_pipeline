#!/usr/bin/env python

"""
Copyright 2021 Robert McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# Import modules
import selenium
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time
import os
import shutil
import sys


def odk_aggregate_log_in_fn(driver):
    """
    Log in to ODK Aggregate using Selenium.
    :param driver: chrome driver executable.
    :return driver: chrome driver executable. """

    # open and log in to ODK Aggregate
    driver.get("https://odktest:odktest@pgb-bas14.nt.gov.au:8443/ODKAggregate")

    return driver


def close_diver_fn(driver):
    """
    Close and quit the chrome driver.
    :param driver: chrome driver executable.
    """
    # close the chrome driver.
    print("driver is about to close")
    time.sleep(5)
    driver.close()
    driver.quit()


def main_routine(chrome_driver, odk_form_list, time_sleep):
    """ Script is called when the command argument variable "remote_desktop" is set to "remote_auto".

     Opens ODK Aggregate via the chrome driver and the Selenium module. The script loops through the "odk_form_list",
     and downloads the export csv by finding elements using their xpath's and verifying that each element is visible
     and in a state that is ready for clicking/selecting. If an element fails - the script and pipeline will end.

    :param time_sleep: integer object (command argument) defining the time chrome driver sleeps between clicking on
    active elements - used when "remote_desktop" is set to "remote_auto".
    :param odk_form_list: list object containing the ODK forms to be downloaded.
    :param chrome_driver: string object (command argument) containing the path to the chrome driver extension - used
    when "remote_desktop" is set to "remote_auto"."""

    # define the Chrome Web driver path
    driver = webdriver.Chrome(chrome_driver)
    time.sleep(5)

    # log in to aggregate
    odk_aggregate_log_in_fn(driver)

    for page in odk_form_list:

        # Find and select the the form dropdown menu.
        time.sleep(time_sleep * 1)
        dropdown_state = WebDriverWait(driver, 20).until(EC.visibility_of_element_located(
            (By.XPATH, "//*[@id='form_and_goal_selection']/tbody/tr/td[2]/select")))

        # determine if the dropdown list element is ready for selecting an option
        if EC.element_selection_state_to_be(dropdown_state, is_selected=True):

            # create a variable of required dropdown selection
            s1 = Select(WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='form_and_goal_selection']/tbody/tr/td[2]/select"))))
            # send program to sleep for n seconds - this allows for the browser to refresh
            time.sleep(2)
            # if dropdown selection is visible, select it.
            if EC.visibility_of(s1):
                # select element
                s1.select_by_visible_text(page)

            else:
                print('could not select from dropdown')
                close_diver_fn(driver)
                print('about to close script')
                import sys
                sys.exit()

            # ------------------------------------------- export button ------------------------------------------------

            # send program to sleep for n seconds - allows for all element to become ready/visible based on the network
            time.sleep(time_sleep * 1)
            # create boolean variable if the element is visible
            export_button_state = WebDriverWait(driver, 20).until(EC.visibility_of_element_located(
                (By.XPATH, "//*[@id='submission_nav_table']/tbody/tr/td[2]/table/tbody/tr/td[2]/button")))
            # send program to sleep for n seconds - this allows for the browser to refresh
            time.sleep(2)

            if export_button_state:
                # create boolean variable if the element is in the required state for clicking
                export_button_element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                    (By.XPATH, "//*[@id='submission_nav_table']/tbody/tr/td[2]/table/tbody/tr/td[2]/button")))
                # send program to sleep for n seconds - this allows for the browser to refresh
                time.sleep(2)
                if EC.element_to_be_clickable(
                        (By.XPATH, "//*[@id='submission_nav_table']/tbody/tr/td[2]/table/tbody/tr/td[2]/button")):
                    # click element
                    export_button_element.click()

                else:
                    print('could not click on the home page button')
                    close_diver_fn(driver)
                    print('about to close script')
                    import sys
                    sys.exit()

                # ---------------------------------------- export csv button -------------------------------------------
                # send program to sleep for n seconds - this allows for the browser to refresh
                time.sleep(time_sleep * 1)
                csv_export_button_state = WebDriverWait(driver, 20).until(EC.visibility_of_element_located(
                    (By.XPATH, "/html/body/div[5]/div/table/tbody/tr/td[7]/button")))
                # send program to sleep for n seconds - this allows for the browser to refresh
                time.sleep(2)
                # print("csv_export_button_state: ", csv_export_button_state)

                if csv_export_button_state:
                    # create boolean variable if the element is in the required state for clicking
                    csv_export_button_element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                        (By.XPATH, "/html/body/div[5]/div/table/tbody/tr/td[7]/button")))
                    # send program to sleep for n seconds - this allows for the browser to refresh
                    time.sleep(2)

                    if EC.element_to_be_clickable(
                            (By.XPATH, "/html/body/div[5]/div/table/tbody/tr/td[7]/button")):
                        # click element
                        csv_export_button_element.click()
                        # print("csv export button_element has been clicked")
                    else:
                        print("could not click csv export button")
                        close_diver_fn(driver)
                        print("about to close script")
                        import sys
                        sys.exit()

                    # -------------------------------------- hyperlink download ----------------------------------------
                    # send program to sleep for n seconds - this allows for the browser to refresh
                    time.sleep(time_sleep * 1)
                    # create boolean variable if the element is visible
                    hyperlink_state = WebDriverWait(driver, 10).until(EC.visibility_of_element_located(
                        (By.XPATH,
                         "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[2]/td/div/div[2]/div/table/tbody/tr[3]/td[4]/div")))
                    # send program to sleep for n seconds - this allows for the browser to refresh
                    time.sleep(2)

                    if hyperlink_state:
                        # print("able to click the hyperlink")
                        # Define an element that you can start scraping when it appears
                        # If the element appears after 5 seconds, break the loop and continue
                        hyperlink_element = WebDriverWait(driver, 2).until(EC.element_to_be_clickable(
                            (By.XPATH,
                             "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[2]/td/div/div[2]/div/table/tbody/tr[3]/td[4]/div")))
                        # print("export_button_element has been located")
                        # send program to sleep for n seconds - this allows for the browser to refresh
                        time.sleep(2)
                        hyperlink_element.click()
                        # print("hyperlink has been clicked")

                    # ------------------------------------ return to home page -------------------------------------
                    # send program to sleep for n seconds - this allows for the browser to refresh
                    time.sleep(time_sleep * 1)
                    home_page_state = WebDriverWait(driver, 20).until(EC.visibility_of_element_located(
                        (By.XPATH,
                         "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[1]/td/table/tbody/tr/td[2]/div/div")))
                    # send program to sleep for n seconds - this allows for the browser to refresh
                    time.sleep(2)
                    # print("home_page_state: ", home_page_state)

                    if home_page_state:
                        # print("able to click the export button")
                        # Define an element that you can start scraping when it appears
                        # If the element appears after 5 seconds, break the loop and continue
                        home_page_element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                            (By.XPATH,
                             "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[1]/td/table/tbody/tr/td[2]/div/div")))
                        # print("export_button_element has been located")
                        # send program to sleep for n seconds - this allows for the browser to refresh
                        time.sleep(2)

                        if EC.element_to_be_clickable(
                                (By.XPATH,
                                 "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[1]/td/table/tbody/tr/td[2]/div/div")):
                            home_page_element.click()
                            # print("home_page_element has been clicked")
                        else:
                            print("could not navigate home")
                            close_diver_fn(driver)
                            print("about to close script")
                            import sys
                            sys.exit()

                    else:
                        print("could not click on the home page button")
                        close_diver_fn(driver)
                        print("about to close script")
                        import sys
                        sys.exit()

                else:
                    print("could not click on the csv export button")
                    close_diver_fn(driver)
                    print("about to close script")
                    import sys
                    sys.exit()
            else:
                print("could not click on the export button")
                close_diver_fn(driver)
                print("about to close script")
                import sys
                sys.exit()

        else:
            print("Not possible")
            close_diver_fn(driver)
            print("about to close script")
            import sys
            sys.exit()

    driver.quit()


if __name__ == "__main__":
    main_routine()
