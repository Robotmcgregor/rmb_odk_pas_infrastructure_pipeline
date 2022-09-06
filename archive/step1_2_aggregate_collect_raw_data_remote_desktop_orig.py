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


def odk_aggregate_log_in_fn(driver, time_sleep):
    """Log in to ODK Aggregate using Selenium."""

    # open and log in to ODK Aggregate
    driver.get("https://odktest:odktest@pgb-bas14.nt.gov.au:8443/ODKAggregate")

    # allow a five second time interval before the next function
    #time.sleep(time_sleep)

    print('Selenium is in control of: ', driver.title)

    return driver


def close_diver_fn(driver):
    # close the chrome driver.
    print('driver is about to close')
    time.sleep(5)
    driver.close()
    driver.quit()


def odk_form_extraction_fn(driver, odk_form_list, chrome_driver, time_sleep):
    for page in odk_form_list:

        odk_aggregate_log_in_fn(driver)

        # Find and select the the form dropdown menu.
        s1 = Select(driver.find_element_by_xpath("//*[@id='form_and_goal_selection']/tbody/tr/td[2]/select"))
        time.sleep(20)
        print('selected the form dropdown')
        # from the form dropdown select by text the required page (loop iteration)
        s1.select_by_visible_text(page)
        time.sleep(20)
        print('selected the appropriate form')

        # todo if table is larger than 1

        try:
            # find and click on the export data button
            driver.find_element_by_xpath(
                "//*[@id='submission_nav_table']/tbody/tr/td[2]/table/tbody/tr/td[2]/button").click()
            print(page, ' located')
            time.sleep(20)

            try:
                # find and click on the export csv button, no filers have been selected.
                driver.find_element_by_xpath("/html/body/div[5]/div/table/tbody/tr/td[7]/button").click()
                print(page, ' html file being prepared for download')
                time.sleep(20)

                driver.find_element_by_xpath(
                    "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[2]/td/div/div[2]/div/table/tbody/tr[3]/td[4]/div").click()
                print('download ', page, 'results csv')

                time.sleep(20)

                print('Navigate back to the Filter submissions page.')

            except NoSuchElementException:
                print(page, 'does not contain data')

        except NoSuchElementException:
            print(page, 'does not contain data')

    close_diver_fn(driver)


def odk_export_csv_checker_fn(search_criteria):
    """ Search for a specific odk csv output.

        :param located_list:
        :param dir_path: string object containing the raw odk output csv files.
        :param search_criteria: string object containing the raw odk file name and type.
        :param temp_dir: string object path to the created output directory (date_time).
        :param veg_list: string object path to the odk veglist excel file (containing botanical and common names).
        :param shrub_list_excel: string object path to the odk veglist excel file (containing a 3P grass list).
        :param pastoral_estate: string object containing the file path to the pastoral estate shapefile. """

    print(os.getcwd())

    path_parent = os.path.dirname(os.getcwd())
    print('path_parent: ', path_parent)
    raw_odk_dir = (path_parent + '\\raw_odk')
    #raw_odk_dir = directory_odk
    print('raw_odk_dir: ', raw_odk_dir)


    user_downloads_dir = os.path.join(os.path.expanduser('~'), 'downloads')
    print('user_downloads_dir: ', user_downloads_dir)
    file_path = (user_downloads_dir + '\\' + search_criteria)
    print(file_path)

    print('=================================')
    print('Searching for: ', search_criteria)

    if not os.path.exists(file_path):
        print(search_criteria, ' not located.')

    else:
        print(search_criteria, ' located, exporting file..........')
        # call the import_script_fn function.
        print(file_path, raw_odk_dir + '\\' + search_criteria)
        print('______________________________________________________')
        print('file_path: ', file_path)
        print('raw_odk_dir:', raw_odk_dir)
        shutil.move(file_path, raw_odk_dir + '\\' + search_criteria)



def main_routine(chrome_driver, odk_form_list, time_sleep):
    """
    Extract the URL's contained within the Star Transect ODK Aggregate .csv for the star transect repeats (transects).
    Open and log into ODK Aggregate using the testodk username and password,
    navigate and open the transect tables, and download the table data as a .html so that it can be imported as a Pandas
    data frame in the following script.
    :param chrome_driver: """

    print('step1_2_aggregate_collect_raw_data_remote_desktop.py INITIATED.')

    # define the Chrome Web driver path
    driver = webdriver.Chrome(chrome_driver)
    time.sleep(5)

    # log in to aggregate
    odk_aggregate_log_in_fn(driver, time_sleep)

    for page in odk_form_list:

        # Find and select the the form dropdown menu.
        #s1 = Select(driver.find_element_by_xpath("//*[@id='form_and_goal_selection']/tbody/tr/td[2]/select"))
        """time.sleep(20)
        print('selected the form dropdown')
        # from the form dropdown select by text the required page (loop iteration)
        s1.select_by_visible_text(page)"""

        time.sleep(time_sleep * 1)
        print('selected the appropriate form')

        for i in range(10):
            try:
                # Define an element that you can start scraping when it appears
                # If the element appears after 5 seconds, break the loop and continue
                WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='form_and_goal_selection']/tbody/tr/td[2]/select")))
                print(page, 'dropdown has been located.')
                s1 = Select(WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//*[@id='form_and_goal_selection']/tbody/tr/td[2]/select"))))
                print(page, 'selected the form dropdown')
                break

            except TimeoutException:
                # If the loading took too long, print message and try again
                print("Loading took too much time!")

        # from the form dropdown select by text the required page (loop iteration)
        #print('select form: ', page)
        #s1.select_by_visible_text(page)


        time.sleep(time_sleep * 1)
        print('when visible click on export button')
        for i in range(10):
            try:
                # Define an element that you can start scraping when it appears
                # If the element appears after 5 seconds, break the loop and continue
                element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//*[@id='submission_nav_table']/tbody/tr/td[2]/table/tbody/tr/td[2]/button")))
                print('element has been located: ', element)
                #element.click()
                #print('export button has been clicked')
                #break

            except TimeoutException:
                # If the loading took too long, print message and try again
                print("Loading took too much time!")

        time.sleep(time_sleep * 1)
        print('when visible click on export csv button')
        for i in range(10):
            try:
                # Define an element that you can start scraping when it appears
                # If the element appears after 5 seconds, break the loop and continue
                element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[5]/div/table/tbody/tr/td[7]/button")))
                print('element has been located: ', element)
                #element.click()
                #print('export csv no filters has been clicked')

                #break

            except TimeoutException:
                # If the loading took too long, print message and try again
                print("Loading took too much time!")

            except NoSuchElementException:
                print(page, 'does not contain data')

        '''time.sleep(time_sleep * 2)
        print('when available click on the data download hyperlink')
        for i in range(10):
            try:
                # Define an element that you can start scraping when it appears
                # If the element appears after 5 seconds, break the loop and continue
                element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[2]/td/div/div[2]/div/table/tbody/tr[3]/td[4]/div")))
                element.click()
                print(page, ' results hyperlink has been clicked')

                break

            except TimeoutException:
                # If the loading took too long, print message and try again
                print("Loading took too much time!")

            except NoSuchElementException:
                print(page, 'does not contain data')'''

        time.sleep(time_sleep)
        print('return to home page')
        for i in range(10):
            try:
                # Define an element that you can start scraping when it appears
                # If the element appears after 5 seconds, break the loop and continue
                element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='mainNav']/tbody/tr[2]/td/div/div[1]/table/tbody/tr[1]/td/table/tbody/tr/td[2]/div/div")))
                #element.click()
                #print(element, ' has been clicked')

                break

            except TimeoutException:
                # If the loading took too long, print message and try again
                print("Loading took too much time!")

            except NoSuchElementException:
                print(page, 'does not contain data')

    odk_results_list = []

    for i in odk_form_list:
        file = i + '_results.csv'
        print(file)
        odk_results_list.append(file)

    for i in odk_results_list:
        # todo if file exists export

        odk_export_csv_checker_fn(i)

    print('step1_2_aggregate_collect_raw_data_remote_desktop.py COMPLETED.')

    return odk_results_list



    #time.sleep(time_sleep)

    #odk_form_extraction_fn(driver, odk_form_list, chrome_driver, time_sleep)

    #odk_results_list = []

    """for i in odk_form_list:
        file = i + '_results.csv'
        print(file)
        odk_results_list.append(file)

    for i in odk_results_list:

        # call the odk_export_csv_checker function to determine if the results file contain an observation/is empty.
        odk_export_csv_checker_fn(i)

    print('step1_2_aggregate_collect_raw_data_remote_desktop.py COMPLETED.')

    return odk_results_list"""

    close_diver_fn(driver)
    print('about to close script')
    time.sleep(20)
    import sys
    sys.exit()

if __name__ == "__main__":
    main_routine()


