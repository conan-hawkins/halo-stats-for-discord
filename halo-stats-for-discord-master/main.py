import pyautogui
import time
import random
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
 


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
    def __init__(self, gamertag = "GT", stats_list = "NA", stat_type = "NA", error_no = 0):
        self.gamertag = gamertag
        self.stats_list = stats_list
        self.stat_type = stat_type
        self.error_no = error_no
    def page_getter(self, gamertag, stat_type):
        options = uc.ChromeOptions()
        #options.add_argument("--headless=new")  # Optional: headless mode
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0.0.0 Safari/537.36")
        driver = uc.Chrome(options=options)
        
        print("Test point 2", gamertag) # !!!==== For testing to be removed later ====!!!
        
        # Open the web page
        driver.get("https://halotracker.com") # /halo-infinite/profile/xbl/"+gamertag+"/overview")
        
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
        # Avoiding bot detection
        #==========================================================
        #driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        #time.sleep(random.uniform(1, 3))
        #driver.execute_script("window.scrollTo(1000, document.body.scrollHeight);")
        
        

        #==========================================================
        # Search gammertags and wait for page to load
        #==========================================================
        # Find and click the search bar
        search_bar = driver.find_element(By.CSS_SELECTOR, "#app > div.trn-wrapper > div.trn-container > div > main > div.container > div.cover > div > div > div.search > div.search-box > div.search-box__bar")
        search_bar.click() # old code
        # Create an action chain
        #actions = ActionChains(driver)
        #actions.move_to_element_with_offset(search_bar, 12, 7).perform() # Move to the element with an offset and click to avoid bot detection
        
        # Make page full screen and wait for it to load
        driver.fullscreen_window()
        time.sleep(random.uniform(1, 2.3)) # Wait to avoid bot detection and allow page to load
        
        # Type 'gamertag' into search bar and press enter key
        pyautogui.typewrite(str(gamertag), interval=0.1) 
        pyautogui.press('enter')
        # Wait for page to load
        time.sleep(3)
        
        # If player isn't found or profile is set to private error catching will occur else the program will continue
        try:
            is_button_visible = driver.find_element(By.XPATH, "/html/body/div[2]/div/div[2]/div[3]/div/main/div[3]/h1").is_displayed() # Looks for error message for player not found
            if is_button_visible == True: # if error message 1 is located
                self.error_no = 2
                driver.quit()  
            else:
                pass # if no error message is located                              
        except:
            print("Test point 3", stat_type) # !!!==== For testing to be removed later ====!!! 
        
        try:
            is_button_visible2 = driver.find_element(By.CSS_SELECTOR, ".private-button-group[data-v-9c0d26eb]").is_displayed() # Looks for error message for private profile
            if is_button_visible2 == True: # if error message 2 is located
                self.error_no = 3
                driver.quit()
            else:
                pass # if no error message is located      
        except:
            print("Test point 3.5", stat_type) # !!!==== For testing to be removed later ====!!!               
        #==========================================================
        # Changes tab based on the stat_type veriable, this is the 
        # only difference between the branches in this file
        #==========================================================
        
        if self.error_no == 2:
           print("passed")
        elif self.error_no == 3:
           print("passed111")      
        elif stat_type == "ranked":
            print("passed222")
            self.stats_getter(driver, gamertag)
        elif stat_type == "stats":
            print("passed333")
            change_tab = driver.find_element(By.XPATH, '//*[@id="app"]/div[2]/div[3]/div/main/div[3]/div[2]/div[1]/div/div/div/ul/li[1]')
            change_tab.click()
            time.sleep(1)
            self.stats_getter(driver, gamertag)
        else:
            self.error_no = 4
            driver.quit()
    #==========================================================#==========================================================
                
                                    #==========================================================
                                    # Pull stats and add them to a list
                                    #==========================================================

    #==========================================================#==========================================================
    def stats_getter(self, driver, gamertag):
            # Get all stat values from webpage
            stats = driver.find_elements(By.CSS_SELECTOR, ".stat .numbers .value[data-v-51a9f6a4]") # 92b64b15 = Edge selector
            self.stats_list = []
            for e in stats:
                e = e.text
                self.stats_list.append(e)
            print (self.stats_list)
            #Close the browser
            driver.quit()

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