import pyautogui
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Made by XxUK D3STROYxX 
# 12/02/2025


#============================================================================#
#                                                                            #
# Automated bot for retrieving halo infinite stats                           #
# Website specific only works with the halotracker.com's HTML and CSS layout #
# Will be working on Halo API version that doesn't need 3rd party website    #
#                                                                            #
#============================================================================#


#==========================================================#==========================================================
           
                                #==========================================================
                                # Makes an object that returns halo stats
                                #==========================================================

#==========================================================#==========================================================
class StatsFind:
    def __init__(self, gamertag = "GT", stats_list = "NA", stat_type = "NA"):
        self.gamertag = gamertag
        self.stats_list = stats_list
        self.stat_type = stat_type
    
    def page_getter(self, gamertag, stat_type):
        # Set up the WebDriver (e.g., Chrome)
        driver = webdriver.Chrome()
        
        print("Test point 2", gamertag) # !!!==== For testing to be removed later ====!!!
        
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
        search_bar = driver.find_element(By.CSS_SELECTOR, "#app > div.trn-wrapper > div.trn-container > div > main > div.container > div.cover > div > div > div.search > div.search-box > div.search-box__bar")
        search_bar.click() 
        # Make page full screen and wait for it to load
        driver.fullscreen_window()
        time.sleep(1)
        # Type 'gamertag' into search bar and press enter key
        pyautogui.typewrite(gamertag, interval=0.0000001)
        pyautogui.press('enter')
        # Wait for page to load
        time.sleep(3)
        
        print("Test point 3", stat_type) # !!!==== For testing to be removed later ====!!! 
        
        #==========================================================
        # Changes tab based on the stat_type veriable, this is the 
        # only difference between the branches in this file
        #==========================================================
        if stat_type == "ranked":
            self.stats_getter(driver, gamertag)
        else:
            change_tab = driver.find_element(By.XPATH, '//*[@id="app"]/div[2]/div[3]/div/main/div[3]/div[2]/div[1]/div/div/div/ul/li[1]')
            change_tab.click()
            time.sleep(1)
            self.stats_getter(driver, gamertag)
            

    #==========================================================#==========================================================
                
                                    #==========================================================
                                    # Pull stats and add them to a list
                                    #==========================================================

    #==========================================================#==========================================================
    def stats_getter(self, driver, gamertag):
            # Get all stat values from webpage
            stats = driver.find_elements(By.CSS_SELECTOR, ".stat .numbers .value[data-v-51a9f6a4]") # 92b64b15 = Edge selector
            self.stats_list = []#1,2,3,4,5,6,7,8,9]
            for e in stats:
                e = e.text
                self.stats_list.append(e)
            print (self.stats_list)
            #Close the browser
            #driver.quit()

StatsFind1 = StatsFind()
if __name__ == "__main__":
    StatsFind1.page_getter(gamertag, stat_type) # Need to look into this error. Code still works but should be resolved


'''       
        =======================================================
        Old or unfinished code will not be in finished project
        =======================================================
        
        bot_detection = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div[3]/div/main/div[3]/p').text
        print (bot_detection)
        if bot_detection == "This Spartan is MIA.":
            search_bar2 = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div[2]/div[2]/nav/ul/li[1]/div')
            search_bar2.click()
            time.sleep(1)
            pyautogui.typewrite(gamertag, interval=0.001)
            pyautogui.press('enter')
            # Wait for page to load
            time.sleep(3)
        else:
            pass
             
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
        
                                                                            # Open overall stats page      
            # Doesn't work look up how to edit query strings    >>     >>    current_url = driver.current_url[:-6] + "overall" https://halotrackeoverall/  
            #                                                   >>           print(current_url)
            #                                                   >>     >>    driver.get(current_url)
        
        # Wait for page to load
        time.sleep(7)
        # Open stats_getter function
        stats_getter(driver, gamertag)
        
        
        # Open screen shot function
        #screen_shot(driver)
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
    driver.quit()

#==========================================================#==========================================================
           
                                #==========================================================
                                # Pull stats and add them to a list
                                #==========================================================

#==========================================================#==========================================================
def stats_getter(driver):
    # Get all stat values from webpage
    stats = driver.find_elements(By.CSS_SELECTOR, ".stat .numbers .value[data-v-92b64b15]")
    stats_list = []
    for e in stats:
        e = e.text
        stats_list.append(e)
    
    # Get all stat titles from webpage
    stats = driver.find_elements(By.CSS_SELECTOR, ".stat .numbers .annotation[data-v-92b64b15], .stat .numbers .name[data-v-92b64b15]")
    name_list = []
    for n in stats:
        n = n.text
        name_list.append(n)
    
    # Zip lists into a dictionary
    result = dict(zip(name_list, stats_list))
    print(result)
    
    #Close the browser
    driver.quit()
    '''