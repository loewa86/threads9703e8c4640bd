import random
import logging
import requests
from typing import AsyncGenerator, Any, Dict
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import subprocess
import shutil
import time
import hashlib
import os
import re
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    ExternalId,
    Url,
    Domain,
)


# Set up logging
logging.basicConfig(level=logging.INFO)

ONLINE_KW_LIST_URL = "https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/refs/heads/main/targets/keywords.txt"
BASE_THREADS_URL = "https://www.threads.net"  # Replace with your desired URL
# Full URL looks like https://www.threads.net/search?q=btc&filter=recent
DEFAULT_OLDNESS_SECONDS = 3600
DEFAULT_MAXIMUM_ITEMS = 200
DEFAULT_MIN_POST_LENGTH = 10


BASE_KEYWORDS = [
    'the', 'of', 'and', 'a', 'in', 'to', 'is', 'that', 'it', 'for', 'on', 'you', 'this', 'with', 'as', 'I', 'be', 'at', 'by', 'from', 'or', 'an', 'have', 'not', 'are', 'but', 'we', 'they', 'which', 'one', 'all', 'their', 'there', 'can', 'has', 'more', 'do', 'if', 'will', 'about', 'up', 'out', 'who', 'get', 'like', 'when', 'just', 'my', 'your', 'what',
    'el', 'de', 'y', 'a', 'en', 'que', 'es', 'la', 'lo', 'un', 'se', 'no', 'con', 'una', 'por', 'para', 'está', 'son', 'me', 'si', 'su', 'al', 'desde', 'como', 'todo', 'está',
    '的', '是', '了', '在', '有', '和', '我', '他', '这', '就', '不', '要', '会', '能', '也', '去', '说', '所以', '可以', '一个',
    'का', 'है', 'हों', 'पर', 'ने', 'से', 'कि', 'यह', 'तक', 'जो', 'और', 'एक', 'हिंदी', 'नहीं', 'आप', 'सब', 'तो', 'मुझे', 'इस', 'को',
    'في', 'من', 'إلى', 'على', 'مع', 'هو', 'هي', 'هذا', 'تلك', 'ون', 'كان', 'لك', 'عن', 'ما', 'ليس', 'كل', 'لكن', 'أي', 'ودي', 'أين',
    'র', 'এ', 'আমি', 'যা', 'তা', 'হয়', 'হবে', 'তুমি', 'কে', 'তার', 'এখন', 'এই', 'কিন্তু', 'মাঠ', 'কি', 'আপনি', 'বাহী', 'মনে', 'তাহলে', 'কেন', 'থাক',
    'o', 'a', 'e', 'de', 'do', 'da', 'que', 'não', 'em', 'para', 'como', 'com', 'um', 'uma', 'meu', 'sua', 'se', 'este', 'esse', 'isto',
    'в', 'и', 'не', 'на', 'что', 'как', 'что', 'он', 'она', 'это', 'но', 'с', 'из', 'по', 'к', 'то', 'да', 'был', 'который', 'кто',
    'の', 'に', 'は', 'を', 'です', 'ます', 'た', 'て', 'いる', 'い', 'この', 'それ', 'あ', '等', 'や', 'も', 'もし', 'いつ', 'よ', 'お',
    'der', 'die', 'das', 'und', 'in', 'zu', 'von', 'mit', 'ist', 'an', 'bei', 'ein', 'eine', 'nicht', 'als', 'auch', 'so', 'wie', 'was', 'oder',
    'le', 'la', 'à', 'de', 'et', 'un', 'une', 'dans', 'ce', 'que', 'il', 'elle', 'est', 's', 'des', 'pour', 'par', 'au', 'en', 'si',
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
]

# List of user agents to choose from
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    # Add more user agents as needed
]

def fetch_keywords_list() -> list:
    # Fetch the list of keywords from the online source, ONLINE_KW_LIST_URL
    try:
        # remote file is a list of comma-separated keywords
        response = requests.get(ONLINE_KW_LIST_URL, timeout=1)
        if response.status_code == 200:
            keywords_list = response.text.split(",")
            # remove any empty strings, and strip leading/trailing whitespace, and \n
            keywords_list = [kw.strip() for kw in keywords_list if kw.strip()]
            return keywords_list
    except Exception as e:
        logging.error(f"Failed to fetch keywords list: {e}")
        return None

def setup_chrome_options():
    options = Options()
    
    # Set up Chrome options
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
        
    # # Set a random user agent
    selected_user_agent = random.choice(user_agents)
    options.add_argument(f"user-agent={selected_user_agent}")
    # logging.info(f"Selected user agent: {selected_user_agent}")

    # make it 'en' by default, localized content can be a problem
    options.add_argument("--lang=en")
    # Disable notifications
    options.add_argument("--disable-notifications")
    # auto accept all cookies
    options.add_argument("--profile-directory=Default")
    options.add_argument("--disable-popup-blocking")
    
    # add headless mode
    options.add_argument("--headless=new")  # Try the new headles
    
    return options

def format_date_string(date_string: str) -> str:
    # Try parsing the date string with milliseconds and 'Z' suffix
    try:
        dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        # If the previous format doesn't match, try parsing without 'Z' suffix
        try:
            dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            # If the previous format doesn't match, try parsing with timezone offset
            try:
                dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                # If the previous format doesn't match, try parsing without milliseconds and with timezone offset
                try:
                    dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z")
                except ValueError:
                    # If none of the formats match, raise an exception
                    raise ValueError(f"Unsupported date format: {date_string}")

    formatted_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return formatted_timestamp


def delete_org_files_in_tmp():
    tmp_folder = "/tmp/"
    target_prefix = ".org"

    try:
        # Check if the /tmp/ folder exists
        if not os.path.exists(tmp_folder):
            logging.info(
                f"[Threads browser Cache] Error: The directory '{tmp_folder}' does not exist."
            )
            return

        # Iterate through the files in /tmp/ folder
        for filename in os.listdir(tmp_folder):
            if filename.startswith(target_prefix):
                file_path = os.path.join(tmp_folder, filename)

                # Try to remove the file
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        logging.info(f"[Threads browser Cache] Deleted file: {filename}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        logging.info(f"[Threads browser Cache] Deleted directory: {filename}")

                # Handle permission errors and other exceptions
                except Exception as e:
                    logging.exception(f"[Threads browser Cache] Error deleting {filename}: {e}")

    except Exception as e:
        logging.exception(f"[Threads browser Cache] An error occurred: {e}")


def delete_core_files():
    current_folder = "/tmp/"
    target_prefix = "core."
    # delete all files in /exorde/ that are starting with core.* (no extension)
    try:
        # check if the /exorde/ folder exists
        if not os.path.exists(current_folder):
            logging.info(f"[Threads browser Cache] Error: The directory '/tmp/' does not exist.")
            return
        
        # iterate through the files in /exorde/ folder
        for filename in os.listdir(current_folder):
            # find all files  starting with core.* (no extension), example core.4000 core.2315331 core.1
            if filename.startswith(target_prefix) and not filename.endswith(".json"):   
                file_path = os.path.join(current_folder, filename)
                # try to remove the file
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        logging.info(f"[Threads browser Cache] Deleted file: {filename}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        logging.info(f"[Threads browser Cache] Deleted directory: {filename}")
                # handle permission errors and other exceptions
                except Exception as e:
                    logging.exception(f"[Threads browser Cache] Error deleting {filename}: {e}")
    except Exception as e:
        logging.exception(f"[Threads browser Cache] An error occurred: {e}")

def calculate_since(max_oldness_seconds: int) -> str:
    # Calculate the timestamp for the specified duration in seconds, UTC+0 timezone
    since = datetime.utcnow() - timedelta(seconds=max_oldness_seconds)

    return since.strftime("%Y-%m-%dT%H:%M:%SZ")

def find_posts(driver):
    # Assuming the HTML content is stored in a variable called 'html_content'
    # use selenium driver to get current page source
    html_content = driver.page_source 
    # DEBUG
    soup = BeautifulSoup(html_content, 'html.parser')
    logging.info(f"[Threads] *** HTML content length: {len(html_content)} ***")
    items = []

    # Find all thread items
    thread_items = soup.find_all('div', class_='x1a2a7pz x1n2onr6')
    logging.info(f"[Threads] Found {len(thread_items)} potential items. ****")
    # print if we found any thread items
    if not thread_items:
        print("No thread items found.")
        return

    for item in thread_items:
        # Extract username
        username_element = item.find('a', class_='x1i10hfl xjbqb8w x1ejq31n xd10rxx x1sy0etr x17r0tee x972fbf xcfux6l x1qhh985 xm0m39n x9f619 x1ypdohk xt0psk2 xe8uvvx xdj266r x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd x16tdsg8 x1hl2dhg xggy1nq x1a2a7pz xp07o12 xzmqwrg x1citr7e x1kdxza xt0b8zv')
        username = username_element.text if username_element else 'Unknown'

        # Extract content
        content_element = item.find('span', class_='x1lliihq x1plvlek xryxfnj x1n2onr6 x1ji0vk5 x18bv5gf x193iq5w xeuugli x1fj9vlw x13faqbe x1vvkbs x1s928wv xhkezso x1gmr53x x1cpjm7i x1fgarty x1943h6x x1i0vuye xjohtrz xo1l8bm xp07o12 x1yc453h xat24cr xdj266r')
        content = content_element.text if content_element else ''

        # Extract timestamp
        timestamp_element = item.find('time', class_='x1rg5ohu xnei2rj x2b8uid xuxw1ft')
        created_at = timestamp_element['datetime'] if timestamp_element else ''

        # Extract URL
        url_element = item.find('a', class_='x1i10hfl xjbqb8w x1ejq31n xd10rxx x1sy0etr x17r0tee x972fbf xcfux6l x1qhh985 xm0m39n x9f619 x1ypdohk xt0psk2 xe8uvvx xdj266r x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd x16tdsg8 x1hl2dhg xggy1nq x1a2a7pz x1lku1pv x12rw4y6 xrkepyr x1citr7e x37wo2f')
        post_url = url_element['href'] if url_element else ''

        created_at= format_date_string(created_at)
        # anonymize author_handle with a hash
        sha1 = hashlib.sha1()
        # Update the hash with the author string encoded to bytest
        try:
            author_ = username
        except:
            author_ = "unknown"
        sha1.update(author_.encode())
        author_sha1_hex = sha1.hexdigest()
        url_recomposed = BASE_THREADS_URL + post_url
        full_content =  content

        item_ = Item(
            content=Content(str(full_content)),
            author=Author(str(author_sha1_hex)),
            created_at=CreatedAt(str(created_at)),
            domain=Domain("threads.net"),
            url=Url(url_recomposed),
        )
        items.append(item_)
    return items


def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get(
                "max_oldness_seconds", DEFAULT_OLDNESS_SECONDS
            )
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get(
                "maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS
            )
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return (
        max_oldness_seconds,
        maximum_items_to_collect,
        min_post_length
    )


def check_and_kill_processes(process_names):
    for process_name in process_names:
        try:
            # Find processes by name
            result = subprocess.check_output(["pgrep", "-f", process_name])
            # If the previous command did not fail, we have some processes to kill
            if result:
                logging.info(f"[Chrome] Killing old processes for: {process_name}")
                subprocess.run(["pkill", "-f", process_name])
        except subprocess.CalledProcessError:
            # If pgrep fails to find any processes, it throws an error. We catch that here and assume no processes are running
            logging.info(f"[Chrome] No running processes found for: {process_name}")


def human_like_scroll(driver,max_scrolls=5):
    # Simulate human-like scrolling behavior
    total_scroll_distance = random.randint(300, 1500)  # Total scroll distance varies
    scroll_segments = random.randint(1, max_scrolls)  # Number of scroll actions
    
    for _ in range(scroll_segments):
        # Randomize scroll direction (mostly down, sometimes up)
        direction = 1 if random.random() < 0.9 else -1
        
        # Randomize scroll distance for each segment
        scroll_distance = direction * random.randint(100, 400)
        
        # Adjust total scroll to stay within reasonable bounds
        if total_scroll_distance + scroll_distance > 3000:
            scroll_distance = max(3000 - total_scroll_distance, 0)
        elif total_scroll_distance + scroll_distance < 0:
            scroll_distance = -total_scroll_distance
        
        total_scroll_distance += scroll_distance
        
        # Execute scroll with easing function for more natural movement
        driver.execute_script(f"""
            window.scrollTo({{
                top: window.pageYOffset + {scroll_distance},
                behavior: 'smooth'
            }});
        """)
        
        # Random pause between scrolls (slightly longer after scrolling up)
        pause_time = random.uniform(0.2, 15) + (0.5 if direction == -1 else 0)
        time.sleep(pause_time)
    
    # Occasional longer pause to simulate reading
    if random.random() < 0.2:
        time.sleep(random.uniform(1, 5))

def convert_spaces_to_percent20(input_string):
    return input_string.replace(" ", "%20")

async def query(parameters: dict) -> AsyncGenerator[Dict[str, Any], None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    yielded_items = 0

    # sleep randomly between 1 and 5 seconds
    time.sleep(random.randint(3, 10))
    
    # Cleanup old chromium processes
    try:
        check_and_kill_processes(["chromium", "chromedriver", "google-chrome"])
    except Exception as e:
        logging.info("[Threads] [Kill old chromium processes] Error: %s", e)
    ## Deleting chromium tmp files taking up space
    try:
        delete_org_files_in_tmp()
    except Exception as e:
        logging.exception(f"[Threads init cleanup] failed: {e}")
    try:
        delete_core_files()
    except Exception as e:
        logging.exception(f"[Threads core. files cleanup] failed: {e}")
    

    # try fetching from the online source
    try:
        logging.info(f"[Threads parameters] fetching keywords list from {ONLINE_KW_LIST_URL}")
        keywords_list = fetch_keywords_list()
        search_keyword = None
    except Exception as e:
        logging.exception(f"[Threads parameters] Keywords list fetch failed: {e}")
        keywords_list = None

    ######################
    #### CHROME SETUP ####
    path_driver = '/usr/local/bin/chromedriver'
    service = Service(path_driver)
    options = setup_chrome_options()    
    driver = webdriver.Chrome(service=service, options=options)
    ######################
    since = calculate_since(max_oldness_seconds)
    consecutive_misses = 0
    try:
        nb_searches = random.randint(1, 5)
        for _ in range(nb_searches):
            time.sleep(random.uniform(1.5, 4.1))
            if yielded_items >= maximum_items_to_collect:
                break
            if keywords_list is not None and keywords_list != []:
                search_keyword = random.choice(keywords_list)
                logging.info(f"[Threads parameters] using online keyword: {search_keyword}")
                # if it fails, use a base keyword
            elif search_keyword is None or random.random() > 0.95: # 5% of the time we want to use a base keyword
                search_keyword = random.choice(BASE_KEYWORDS)
                logging.info(f"[Threads parameters] using base keyword: {search_keyword}")

            # Preprocess the search keyword: remove any leading/trailing whitespace, remove any newlines
            search_keyword = search_keyword.strip().replace("\n", "")
            # if here is a (*) pattern, remove it. ex: Le Bitcoin (btc) ->  Le Bitcoin
            search_keyword = re.sub(r'\s*\([^)]*\)', '', search_keyword)

            ## 1. FIRST WE GO TO HOME PAGE
            logging.info(f"[Threads] Opening Home URL: {BASE_THREADS_URL}")
            driver.get(BASE_THREADS_URL)

            ## 1.a. try to accept cookies
            # sleep 3s
            if _ == 0:
                time.sleep(random.uniform(2, 4))
                try:
                    timeout = 5
                    # Find the last div with the specific classes
                    last_div = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, "(//div[contains(@class, 'x6bh95i') and contains(@class, 'x13fuv20') and contains(@class, 'x178xt8z') and contains(@class, 'x1p5oq8j') and contains(@class, 'xxbr6pl') and contains(@class, 'xwxc41k') and contains(@class, 'xbbxn1n')])[last()]"))
                    )
                    # print the content of last div
                    logging.info(f"[Threads] Last div content: {last_div.text}")
                    # Find the first div within the last div that has role="button" and tabindex="0"
                    button = last_div.find_element(By.XPATH, ".//div[@role='button' and @tabindex='0']")
                    # Use ActionChains to move to the element and click with a small delay
                    actions = ActionChains(driver)
                    actions.move_to_element(button)
                    time.sleep(random.uniform(0.07, 0.42))  # Random delay between 0.1 seconds
                    actions.click(button).perform()
                    logging.info("[Threads] Cookie banner button clicked using ActionChains")
                except Exception as e:
                    logging.error(f"[Threads] Error clicking cookie banner button with ActionChains.")
            else:
                time.sleep(2)


            # 1.b scroll a little bit
            human_like_scroll(driver,2)
            
            logging.info("[Threads] Scrolling done, waiting for search button to be clickable")

            # Wait for the search button to be clickable
            max_retries = 2
            
            for attempt in range(max_retries):
                try:
                    search_button = WebDriverWait(driver, 1).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='/search']"))
                    )
                    logging.info("[Threads] Search button is clickable ***")
                    search_button.click()
                    break
                except TimeoutException:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(int(attempt + 1))
                    print(f"Search bar Attempt {attempt + 1} failed, retrying...")          
                    logging.info("[Threads] Search bar is unavailable ***")
            

            # 3. Click on the search button
            search_button.click()
            logging.info("[Threads] Clicked on the search button")
            logging.info(f"[Threads] Current URL: {driver.current_url}")
            # 3.a Wait for the URL to change to the search page
            
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    WebDriverWait(driver, 5).until(
                        EC.url_to_be("https://www.threads.net/search")
                    )
                except TimeoutException:
                    if attempt == max_retries - 1:
                        raise
                    logging.info("[Threads] Search bar is unavailable ***")
                    search_button.click()
                    logging.info(f"[Threads] Clicked on the search button, attempt {attempt + 1}")
                            

            # if still not on search, we stop and quit.
            if driver.current_url != "https://www.threads.net/search":
                logging.info("[Threads] Not on search page, quitting.")
                break
            logging.info("[Threads] URL changed to the search page")
                           

            # 4. TYPE THE SEARCH KEYWORD ORGANICALLY
            time.sleep(random.uniform(0.4, 1.2))
            # find the first <input class="x1i10hfl x9f619 xggy1nq x1s07b3s x1kdt53j x1a2a7pz x1ggkfyp x972fbf xcfux6l x1qhh985 xm0m39n xp07o12 x1i0vuye xjohtrz x5yr21d x1yc453h xh8yej3 x1e899rk x1bn1fsv xtilpmw x1ad04t7 x1glnyev x1ix68h3 x19gujb8" dir="ltr" autocapitalize="off" autocomplete="off" placeholder="Search" spellcheck="false" type="search" value="" tabindex="0"> 
            # look for the first "input" element with  type="search"
            try:
                search_input = driver.find_element(By.CSS_SELECTOR, "input[type='search']")
                search_input.click()
                # type the search keyword with a delay between each character of 0.05s & 0.15s
                for letter in search_keyword:
                    search_input.send_keys(letter)
                    time.sleep(random.uniform(0.07, 0.42))
                search_input.send_keys(Keys.RETURN)
            except:
                print("No parent form element found.")

            time.sleep(random.uniform(0.2, 2))
            # 4.b scroll a little bit
            human_like_scroll(driver,4)
            
            
            # 5. EXTRACT POSTS
            posts = find_posts(driver)
            logging.info(f"[Threads] Fetching posts for keyword '{search_keyword}' since {since}")
            if posts is None:
                logging.info(f"[Threads] No posts found for this keyword. Moving on.")
                consecutive_misses += 1
                # if we have 3 consecutive misses, we stop, sleep randomly between 3 and 10 seconds
                if consecutive_misses >= 3:
                    await time.sleep(int(consecutive_misses + 1))
                    break
                continue

            for post in posts:
                try:
                    # double check if the post['created_at'] is within the last max_oldness_seconds
                    if post['created_at'] < since:
                        continue
                    if yielded_items >= maximum_items_to_collect:
                        break
                    # display only the first 500 characters of the content and wihtout \n
                    display_content = post['content'][:250].replace("\n", " ")
                    logging.info(f"[Threads] Found post: {post['url']} created at {post['created_at']} with content: {display_content} by authorID: {post['author']}")
                    yielded_items += 1
                    # reset consecutive_misses
                    consecutive_misses = 0
                    yield post
                except Exception as e:
                    logging.exception(f"[Threads] Error processing post: {e}")
    except Exception as e:
        logging.exception(f"[Threads] Error processing posts: {e}")
    finally:
        driver.quit()
    
