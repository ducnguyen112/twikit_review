from xvfbwrapper import Xvfb
import pyotp
from playwright.async_api import async_playwright
import asyncio
from typing import Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XComLogin:
    async def login(self, email: str, password: str, totp_secret: str, email_url: str):
        async with async_playwright() as p:
            # Launch the browser
            browser = await p.chromium.launch(headless=False)  # Set headless=True to run without UI
            context = await browser.new_context()
            page = await context.new_page()
            
            # Navigate to X.com login page
            await page.goto("https://x.com")
            await page.goto("https://x.com/login")

            # Fill in the login form
            await page.fill('input[name="text"]', email)  # Input field for email/username
            # page.click('div[role="button"][data-testid="LoginForm_Login_Button"]')  # Click the "Next" button
            await page.click('//*[@id="layers"]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div[2]/div/div/div/button[2]', delay=2)
            # Wait for the password field to appear
            await page.wait_for_selector('input[name="password"]')
            await page.fill('input[name="password"]', password)  # Input field for password
            
            # Click the login button
            await page.click('//*[@id="layers"]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div[2]/div[2]/div/div[1]/div/div/button', delay=2)
            
            await page.wait_for_selector('//*[@id="layers"]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div[2]/div[1]/div/div[2]/label/div/div[2]/div/input')

            await page.fill('//*[@id="layers"]/div/div/div/div/div/div/div[2]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/label/div/div[2]/div/input', pyotp.TOTP(totp_secret).now())
            # Wait for navigation or a successful login indicator

            await page.click('//*[@id="layers"]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div[2]/div[2]/div/div/div/button', delay=2.0)
            # Check if login was successful
            await page.wait_for_timeout(5000)  # Adjust timeout as needed
            if page.url != "https://x.com/login":
                print("Login successful!")
            else:
                print("Login failed. Please check your credentials.")

            # Get cookies
            cookies = await context.cookies()
            ct0 = next((c['value'] for c in cookies if c['name'] == 'ct0'), None)
            auth_token = next((c['value'] for c in cookies if c['name'] == 'auth_token'), None)
            print(f"ct0: {ct0}")
            print(f"auth_token: {auth_token}")

            if not ct0 or not auth_token:
                raise Exception("Failed to get authentication cookies")
            # Close the browser
            # await browser.close()
            return ct0, auth_token

async def test_login():
    """Test the login functionality"""
    login = XComLogin()
    try:
        ct0, auth_token = await login.login(
            email="ELohmeyer50991",
            password="CZz9ieUXse2",
            totp_secret="RFDAKMN7DDIATZ4H"
        )
        logger.info(f"Login successful! ct0: {ct0[:10]}..., auth_token: {auth_token[:10]}...")
    except Exception as e:
        logger.error(f"Login test failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_login())