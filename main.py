import pyautogui
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from PIL import Image


# Made by XxUK D3STROYxX 
# 12/02/2025


#============================================================================#
#                                                                            #
# Automated screenshot bot for retrieving ranked halo infinite stats         #
# Website specific only works with the halotracker.com's HTML and CSS layout #
#                                                                            #
#============================================================================#


#==========================================================#==========================================================
           
                                #==========================================================
                                # Makes a function that returns ranked stats
                                #==========================================================

#==========================================================#==========================================================
def ranked_com(gamertag):
    # Set up the WebDriver (e.g., Chrome)
    driver = webdriver.Edge()

    # Open the web page 
    driver.get('https://halotracker.com/')

#==========================================================
# Coockie pop up handling
#==========================================================
    # Wait for the cookie banner to appear
    wait = WebDriverWait(driver, 7)
    x = wait.until(EC.presence_of_element_located((By.ID, "qc-cmp2-ui")))


    # Click the cookie
    accept_button = driver.find_element(By.CLASS_NAME, "css-47sehv")
    accept_button.click()

#==========================================================
# Search gammertags and wait for page to load
#==========================================================
    # Find and click the search bar
    search_bar = driver.find_element(By.CLASS_NAME, "search-box__bar")
    search_bar.click() 

    # Type 'gamertag' into search bar and press enter key
    pyautogui.typewrite(gamertag, interval=0.000001)
    pyautogui.press('enter')
    
    # Wait for page to load
    time.sleep(6)
    screen_shot(driver)

#==========================================================#==========================================================
           
                                #==========================================================
                                # Makes a function that returns all stats
                                #==========================================================

#==========================================================#==========================================================
def stats_com(gamertag):
    # Set up the WebDriver (e.g., Chrome)
    driver = webdriver.Edge()

    # Open the web page 
    driver.get('https://halotracker.com/')

#==========================================================
# Coockie pop up handling
#==========================================================
    # Wait for the cookie banner to appear
    wait = WebDriverWait(driver, 7)
    x = wait.until(EC.presence_of_element_located((By.ID, "qc-cmp2-ui")))


    # Click the cookie
    accept_button = driver.find_element(By.CLASS_NAME, "css-47sehv")
    accept_button.click()

#==========================================================
# Search gammertags and wait for page to load
#==========================================================
    # Find and click the search bar
    search_bar = driver.find_element(By.CLASS_NAME, "search-box__bar")
    search_bar.click() 

    # Type 'gamertag' into search bar and press enter key
    pyautogui.typewrite(gamertag, interval=0.000001)
    pyautogui.press('enter')
    
    # Wait for page to load
    time.sleep(7)
    screen_shot(driver)

#==========================================================#==========================================================
           
                                #==========================================================
                                # Prepare screen for screenshot then Screenshot and crop
                                #==========================================================

#==========================================================#==========================================================
def screen_shot(driver):
    # Zoom out the webpage and make full screen
    driver.fullscreen_window()
    driver.execute_script("document.body.style.zoom='0.9'")

    # Take a screenshot and save it to a file
    screenshot_path = 'webpage_screenshot.png'
    driver.save_screenshot(screenshot_path)

    # Open the screenshot and crop, then save
    img = Image.open("webpage_screenshot.png") 
    img_res = img.crop((520, 400, 1500, 900))
    img_res.save('cropped_example.png')

    #Close the browser
   # driver.quit()