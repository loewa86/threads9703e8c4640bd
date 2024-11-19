import subprocess
import hashlib
import shutil
import os
import random
import logging
import requests
from typing import AsyncGenerator, Any, Dict
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Url,
    Domain,
)


# Set up logging
logging.basicConfig(level=logging.INFO)

ONLINE_KW_LIST_URL = "https://raw.githubusercontent.com/exorde-labs/TestnetProtocol/refs/heads/main/targets/keywords.txt"
BASE_THREADS_URL = "https://www.threads.net"  # Replace with your desired URL
# Full URL looks like https://www.threads.net/search?q=btc&filter=recent
DEFAULT_OLDNESS_SECONDS = 3600
DEFAULT_MAXIMUM_ITEMS = 20
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
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-dev-shm-usage")
    
    # Set a random user agent
    selected_user_agent = random.choice(user_agents)
    options.add_argument(f"user-agent={selected_user_agent}")
    logging.info(f"Selected user agent: {selected_user_agent}")

    # add headless mode
    options.add_argument("--headless")
    
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
    current_folder = "/exorde/"
    target_prefix = "core."
    # delete all files in /exorde/ that are starting with core.* (no extension)
    try:
        # check if the /exorde/ folder exists
        if not os.path.exists(current_folder):
            logging.info(f"[Threads browser Cache] Error: The directory '/exorde/' does not exist.")
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
    soup = BeautifulSoup(html_content, 'html.parser')
    items = []

    # Find all thread items
    thread_items = soup.find_all('div', class_='x1a2a7pz x1n2onr6')
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


def convert_spaces_to_percent20(input_string):
    return input_string.replace(" ", "%20")

async def query(parameters: dict) -> AsyncGenerator[Dict[str, Any], None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    yielded_items = 0
    
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

    ############################
    # DEBUG
    # Set up ChromeDriver   
    # from webdriver_manager.chrome import ChromeDriverManager
    # service = Service(ChromeDriverManager().install())    
    # # Set up Chrome options
    # options = setup_chrome_options()    
    # driver = webdriver.Chrome(service=service, options=options)

    driver_path = '/usr/local/bin/chromedriver'
    logging.info(f"Opening driver from path = {driver_path}")
    options = setup_chrome_options()    
    driver = webdriver.Chrome(service=Service(driver_path), options=options)

    ############################
    since = calculate_since(max_oldness_seconds)  
    consecutive_misses = 0
    try:
        for _ in range(3):
            # random sleep between 0.5 and 2 seconds
            await asyncio.sleep(random.uniform(0.5, 2))
            if yielded_items >= maximum_items_to_collect:
                break
            if keywords_list is not None and keywords_list != []:
                search_keyword = random.choice(keywords_list)
                logging.info(f"[Threads parameters] using online keyword: {search_keyword}")
                # if it fails, use a base keyword
            elif search_keyword is None or random.random() > 0.95: # 5% of the time we want to use a base keyword
                search_keyword = random.choice(BASE_KEYWORDS)
                logging.info(f"[Threads parameters] using base keyword: {search_keyword}")

            # add &filter=recent 95% of the time
            search_keyword = convert_spaces_to_percent20(search_keyword)
            # add &filter=recent 95% of the time
            effective_URL = BASE_THREADS_URL + f"/search?q={search_keyword}"
            if random.random() > 0.05: # 95% of the time we want to filter by recent, 5% of the time we look at top items
                effective_URL += "&filter=recent"

            # Open a webpage
            logging.info(f"[Threads] Opening URL: {effective_URL}")
            driver.get(effective_URL)
            # sleep 1s
            driver.implicitly_wait(1)
            
            # Your automation code goes here
            posts = find_posts(driver)
            logging.info(f"[Threads] Fetching posts for keyword '{search_keyword}' since {since}")
            if posts is None:
                logging.info(f"[Threads] No posts found for this keyword. Moving on.")
                consecutive_misses += 1
                # if we have 3 consecutive misses, we stop, sleep randomly between 3 and 10 seconds
                if consecutive_misses >= 3:
                    await asyncio.sleep(random.randint(3, 10))
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
    
