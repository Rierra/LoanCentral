import praw
import psycopg2
import re
import time
import logging
import os
from datetime import datetime, timedelta  # Added missing timedelta import
from dotenv import load_dotenv
import threading
import sys
from decimal import Decimal  # Import Decimal type
import traceback  # For better error reporting

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("LoanCentral.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("LoanCentral")

# Reddit API credentials
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    username=os.getenv("REDDIT_USERNAME"),
    password=os.getenv("REDDIT_PASSWORD"),
    user_agent=os.getenv("USER_AGENT")
)

# PostgreSQL connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        logger.error(traceback.format_exc())  # Added consistent error logging
        return None

# Initialize database tables if they don't exist
def init_database():
    conn = get_db_connection()
    if not conn:
        return False
    
    cur = conn.cursor()
    try:
        # Create loans table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS loans (
                id SERIAL PRIMARY KEY,
                loan_id TEXT UNIQUE,
                lender TEXT NOT NULL,
                borrower TEXT NOT NULL,
                amount NUMERIC NOT NULL,
                currency TEXT NOT NULL,
                date_created TIMESTAMP NOT NULL,
                original_thread TEXT NOT NULL,
                status TEXT DEFAULT 'active',
                amount_repaid NUMERIC DEFAULT 0,
                last_updated TIMESTAMP
            )
        ''')
        
        # Create users table to track user statistics
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                loans_as_borrower INTEGER DEFAULT 0,
                loans_as_lender INTEGER DEFAULT 0,
                amount_borrowed NUMERIC DEFAULT 0,
                amount_lent NUMERIC DEFAULT 0,
                amount_repaid NUMERIC DEFAULT 0,
                unpaid_loans INTEGER DEFAULT 0,
                unpaid_amount NUMERIC DEFAULT 0,
                last_updated TIMESTAMP
            )
        ''')

        # Create indexes for better performance
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_loans_lender ON loans(lender);
            CREATE INDEX IF NOT EXISTS idx_loans_borrower ON loans(borrower);
            CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(status);
            CREATE INDEX IF NOT EXISTS idx_loans_date_created ON loans(date_created);
        ''')
        
        conn.commit()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Database initialization error: {e}")
        logger.error(traceback.format_exc())  # Added consistent error logging
        return False
    finally:
        cur.close()
        conn.close()

# Generate a unique loan ID
def generate_loan_id():
    current_time = int(time.time())
    return f"{current_time}"

# Process $loan command
def process_loan_command(comment):
    loan_regex = r'\$loan\s+(\d+(?:\.\d+)?)\s+([A-Z]{3})'
    match = re.search(loan_regex, comment.body, re.IGNORECASE)
    
    if not match:
        return
    
    lender = comment.author.name.lower()
    post = comment.submission
    borrower = post.author.name.lower()
    amount = Decimal(match.group(1))  # Convert to Decimal instead of float
    currency = match.group(2).upper()
    
    if borrower == lender:
        logger.warning(f"User {lender} attempted to lend to themselves")
        return
    
    loan_id = generate_loan_id()
    thread_url = f"https://www.reddit.com{post.permalink}"
    
    # Instead of inserting loan immediately, just reply with confirmation instructions
    try:
        # Reply to the comment
        reply_text = f'''
I've seen that u/{lender} is offering {amount:.2f} {currency} to u/{borrower}!

u/{borrower} needs to confirm this transaction using:

```
$confirm /u/{lender} {amount:.2f} {currency}
```

The loan will only be registered in the database after confirmation. This helps ensure that the money was actually sent and received.
'''
        comment.reply(reply_text)
        logger.info(f"Loan offer recorded: {lender} is offering {amount} {currency} to {borrower}")
        
    except Exception as e:
        logger.error(f"Error processing loan command: {e}")
        logger.error(traceback.format_exc())  # Log full stack trace

# Process $confirm command
# Process $confirm command
def process_confirm_command(comment):
    # First, check if the command is in a code block and extract it
    code_block_regex = r'```\s*(.*?)\s*```'
    code_blocks = re.findall(code_block_regex, comment.body, re.DOTALL)
    
    # Text to search - either the code block content or the full comment body
    text_to_search = code_blocks[0] if code_blocks else comment.body
    
    # Now search for the confirm command
    confirm_regex = r'\$confirm\s+\/u\/([^\s]+)\s+(\d+(?:\.\d+)?)\s+([A-Z]{3})'
    match = re.search(confirm_regex, text_to_search, re.IGNORECASE)
    
    if not match:
        # Try alternate format without /u/
        alt_confirm_regex = r'\$confirm\s+u\/([^\s]+)\s+(\d+(?:\.\d+)?)\s+([A-Z]{3})'
        match = re.search(alt_confirm_regex, text_to_search, re.IGNORECASE)
        if not match:
            return
    
    borrower = comment.author.name.lower()
    lender = match.group(1).lower()
    amount = Decimal(match.group(2))
    currency = match.group(3).upper()
    
    # Rest of the function remains the same...
    # This is where we'll actually create the loan
    loan_id = generate_loan_id()
    post = comment.submission
    thread_url = f"https://www.reddit.com{post.permalink}"
    
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        # Insert loan
        cur.execute('''
            INSERT INTO loans 
            (loan_id, lender, borrower, amount, currency, date_created, original_thread, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (loan_id, lender, borrower, amount, currency, datetime.now(), thread_url, 'confirmed'))
        
        db_id = cur.fetchone()[0]
        
        
        # Update user statistics
        # Update lender stats
        cur.execute('''
            INSERT INTO users 
            (username, loans_as_lender, amount_lent, last_updated)
            VALUES (%s, 1, %s, %s)
            ON CONFLICT (username) 
            DO UPDATE SET 
                loans_as_lender = users.loans_as_lender + 1,
                amount_lent = users.amount_lent + %s,
                last_updated = %s
        ''', (lender, amount, datetime.now(), amount, datetime.now()))
        
        # Update borrower stats
        cur.execute('''
            INSERT INTO users 
            (username, loans_as_borrower, amount_borrowed, last_updated)
            VALUES (%s, 1, %s, %s)
            ON CONFLICT (username) 
            DO UPDATE SET 
                loans_as_borrower = users.loans_as_borrower + 1,
                amount_borrowed = users.amount_borrowed + %s,
                last_updated = %s
        ''', (borrower, amount, datetime.now(), amount, datetime.now()))
        
        conn.commit()
        logger.info(f"Confirmed loan: {borrower} confirmed receiving {amount} {currency} from {lender}")
        
        # Reply to the comment
        reply_text = f'''
Confirmed: u/{borrower} has confirmed receiving {amount:.2f} {currency} from u/{lender}.

If you wish to mark this loan repaid later, you can use:

```
$paid_with_id {db_id} {amount:.2f} {currency}
```

Processing time: {time.time() - comment.created_utc:.4f} seconds

If the loan transaction did not work out and needs to be refunded then the *lender* should reply to this comment with 'Refunded' and moderators will be automatically notified
'''
        comment.reply(reply_text)
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing confirm command: {e}")
        logger.error(traceback.format_exc())  # Log full stack trace
    finally:
        cur.close()
        conn.close()

# Process $paid_with_id command
def process_paid_command(comment):
    # First, check if the command is in a code block and extract it
    code_block_regex = r'```\s*(.*?)\s*```'
    code_blocks = re.findall(code_block_regex, comment.body, re.DOTALL)
    
    # Text to search - either the code block content or the full comment body
    text_to_search = code_blocks[0] if code_blocks else comment.body
    
    # Now search for the paid command
    paid_regex = r'\$paid_with_id\s+(\d+)\s+(\d+(?:\.\d+)?)\s+([A-Z]{3})'
    match = re.search(paid_regex, text_to_search, re.IGNORECASE)
    
    if not match:
        return
    
    lender = comment.author.name.lower()
    loan_id = match.group(1)
    amount_paid = Decimal(match.group(2))  # Convert to Decimal instead of float
    currency = match.group(3).upper()
    
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        # Find the loan
        cur.execute('''
            SELECT id, borrower, amount, amount_repaid, currency
            FROM loans
            WHERE id = %s AND lender = %s
        ''', (loan_id, lender))
        
        result = cur.fetchone()
        if not result:
            logger.warning(f"No matching loan found for payment: ID {loan_id} by {lender}")
            comment.reply(f"Error: Could not find a loan with ID {loan_id} where you are the lender.")
            return
        
        db_id, borrower, loan_amount, already_repaid, loan_currency = result
        
        # Ensure all values are Decimal for calculations
        loan_amount = Decimal(loan_amount) if not isinstance(loan_amount, Decimal) else loan_amount
        already_repaid = Decimal(already_repaid) if not isinstance(already_repaid, Decimal) else already_repaid
        
        if loan_currency != currency:
            comment.reply(f"Error: Currency mismatch. The loan was in {loan_currency}, but you specified {currency}.")
            return
        
        # Get loan details before the update for the response
        cur.execute('''
            SELECT lender, borrower, amount, amount_repaid, currency, original_thread
            FROM loans
            WHERE id = %s
        ''', (loan_id,))
        loan_before = cur.fetchone()
        
        # Update the loan with the amount paid
        new_repaid_amount = already_repaid + amount_paid
        new_status = 'repaid' if new_repaid_amount >= loan_amount else 'partially_repaid'
        
        cur.execute('''
            UPDATE loans
            SET amount_repaid = %s,
                status = %s,
                last_updated = %s
            WHERE id = %s
        ''', (new_repaid_amount, new_status, datetime.now(), loan_id))
        
        # Update user statistics
        cur.execute('''
            UPDATE users
            SET amount_repaid = amount_repaid + %s,
                last_updated = %s
            WHERE username = %s
        ''', (amount_paid, datetime.now(), borrower))
        
        # Update unpaid loans count and amount if the loan is now fully paid
        if new_status == 'repaid':
            cur.execute('''
                UPDATE users
                SET unpaid_loans = GREATEST(unpaid_loans - 1, 0),
                    unpaid_amount = GREATEST(unpaid_amount - %s, 0),
                    last_updated = %s
                WHERE username = %s
            ''', (loan_amount, datetime.now(), borrower))
        
        # Get loan details after the update
        cur.execute('''
            SELECT lender, borrower, amount, amount_repaid, currency, original_thread
            FROM loans
            WHERE id = %s
        ''', (loan_id,))
        loan_after = cur.fetchone()
        
        conn.commit()
        logger.info(f"Payment recorded: {borrower} repaid {amount_paid} {currency} to {lender}")
        
        # Generate the response message
        response = f"u/{borrower} has now repaid u/{lender} {amount_paid:.2f} {currency}.\n\n"
        response += "Loan before this transaction:\n\n"
        response += "|Lender|Borrower|Amount Given|Amount Repaid|Unpaid?|Original Thread|\n"
        response += "|---|---|---|---|---|---|\n"
        response += f"|{loan_before[0]}|{loan_before[1]}|{loan_before[2]:.2f} {loan_before[4]}|{loan_before[3]:.2f} {loan_before[4]}|{'Yes' if loan_before[3] < loan_before[2] else 'No'}|[Link]({loan_before[5]})|\n\n"
        
        response += "Loan after this transaction:\n\n"
        response += "|Lender|Borrower|Amount Given|Amount Repaid|Unpaid?|Original Thread|\n"
        response += "|---|---|---|---|---|---|\n"
        response += f"|{loan_after[0]}|{loan_after[1]}|{loan_after[2]:.2f} {loan_after[4]}|{loan_after[3]:.2f} {loan_after[4]}|{'Yes' if loan_after[3] < loan_after[2] else 'No'}|[Link]({loan_after[5]})|\n\n"
        
        remaining = loan_amount - new_repaid_amount
        if remaining > 0:
            response += f"amount specified: {amount_paid:.2f} {currency}, remaining: {remaining:.2f} {currency}"
        else:
            response += f"amount specified: {amount_paid:.2f} {currency}, remaining: 0.00 {currency}"
        
        comment.reply(response)
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing paid command: {e}")
        logger.error(traceback.format_exc())  # Log full stack trace
    finally:
        cur.close()
        conn.close()

# Process $refunded command
def process_refund_command(comment):
    # Check if this is a reply to a loan bot comment
    if not comment.parent().author or comment.parent().author.name.lower() != os.getenv("REDDIT_USERNAME").lower():
        return
    
    if "refunded" not in comment.body.lower():
        return
    
    # Extract the loan information from the parent comment
    parent_body = comment.parent().body
    loan_regex = r'u\/([^\s]+) has confirmed receiving (\d+(?:\.\d+)?)\s+([A-Z]{3}) from u\/([^\s\.]+)'
    match = re.search(loan_regex, parent_body)
    
    if not match:
        return
    
    borrower = match.group(1).lower()
    amount = Decimal(match.group(2))  # Convert to Decimal instead of float
    currency = match.group(3)
    lender = match.group(4).lower()
    
    # Verify the refund command is from the lender
    if comment.author.name.lower() != lender:
        comment.reply("Only the lender can mark a loan as refunded.")
        return
    
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        # Find the relevant loan
        cur.execute('''
            SELECT id FROM loans
            WHERE lender = %s AND borrower = %s AND amount = %s AND currency = %s
            ORDER BY date_created DESC
            LIMIT 1
        ''', (lender, borrower, amount, currency))
        
        result = cur.fetchone()
        if not result:
            logger.warning(f"No matching loan found for refund: {lender} to {borrower} for {amount} {currency}")
            comment.reply(f"Error: Could not find a matching loan from you to u/{borrower} for {amount} {currency}.")
            return
        
        loan_id = result[0]
        
        # Update the loan status to refunded
        cur.execute('''
            UPDATE loans
            SET status = 'refunded',
                last_updated = %s
            WHERE id = %s
        ''', (datetime.now(), loan_id))
        
        # Update user statistics - properly update based on existing values
        cur.execute('''
            UPDATE users
            SET loans_as_lender = GREATEST(loans_as_lender - 1, 0),
                amount_lent = GREATEST(amount_lent - %s, 0),
                last_updated = %s
            WHERE username = %s
        ''', (amount, datetime.now(), lender))
        
        cur.execute('''
            UPDATE users
            SET loans_as_borrower = GREATEST(loans_as_borrower - 1, 0),
                amount_borrowed = GREATEST(amount_borrowed - %s, 0),
                last_updated = %s
            WHERE username = %s
        ''', (amount, datetime.now(), borrower))
        
        conn.commit()
        logger.info(f"Loan refunded: {lender} refunded {amount} {currency} to {borrower}")
        
        # Reply to the comment
        comment.reply(f"Loan marked as refunded. The loan from u/{lender} to u/{borrower} for {amount:.2f} {currency} has been removed from both users' statistics.")
        
        # Notify moderators
        subreddit = reddit.subreddit(os.getenv("SUBREDDIT"))
        subject = f"Loan Refunded - {lender} to {borrower}"
        message = f"A loan has been marked as refunded:\n\nLender: u/{lender}\nBorrower: u/{borrower}\nAmount: {amount:.2f} {currency}\n\nLink to comment: https://www.reddit.com{comment.permalink}"
        subreddit.message(subject, message)
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing refund command: {e}")
        logger.error(traceback.format_exc())  # Log full stack trace
    finally:
        cur.close()
        conn.close()

# ----- Account Stats Command -----
# ----- Account Stats Command -----
def process_stats_command(comment):
    # Updated regex to make /u/ optional by using (?:/u/|u/) pattern
    m = re.search(r"\$stats\s+(?:/u/|u/)([^\s]+)", comment.body, re.IGNORECASE)
    if not m:
        return
    
    user = m.group(1).lower()
    
    try:
        redditor = reddit.redditor(user)
        # Fetch comments (up to PRAW limit to avoid rate limits)
        comments = list(redditor.comments.new(limit=100))
        
        now = datetime.utcnow()
        # Total and recent comments
        total_comments = len(comments)
        cutoff = now - timedelta(days=180)
        recent_comments = [c for c in comments if datetime.utcfromtimestamp(c.created_utc) >= cutoff]
        
        if not comments:
            comment.reply(f"No comments found for u/{user}.")
            return
            
        # Oldest & newest
        dates = [datetime.utcfromtimestamp(c.created_utc) for c in comments]
        newest = max(dates).date()
        eldest = min(dates).date()
        
        # Unique comment gaps
        sorted_dates = sorted(dates)
        gaps = [(t2 - t1).total_seconds() for t1, t2 in zip(sorted_dates, sorted_dates[1:])]
        avg_gap = sum(gaps) / len(gaps) / 86400 if gaps else 0
        max_gap = max(gaps) / 86400 if gaps else 0
        
        # Daily activity
        days = sorted(list({dt.date() for dt in dates}))
        day_gaps = [(t2 - t1).days for t1, t2 in zip(days, days[1:])]
        avg_day_gap = sum(day_gaps) / len(day_gaps) if day_gaps else 0
        max_day_gap = max(day_gaps) if day_gaps else 0
        
        # Karma by subreddit
        karma_map = {}
        for c in comments:
            sr = c.subreddit.display_name
            karma_map[sr] = karma_map.get(sr, 0) + c.score
        top_3 = sorted(karma_map.items(), key=lambda x: x[1], reverse=True)[:3]
        
        # Account info
        post_karma = redditor.link_karma
        comment_karma = redditor.comment_karma
        combined = post_karma + comment_karma
        created = datetime.utcfromtimestamp(redditor.created_utc)
        age_days = (now - created).days
        age_years = age_days / 365
        verified = getattr(redditor, 'has_verified_email', False)
        
        # Build reply
        reply = [f"**Account Statistics for u/{user}:**\n"]
        reply.append(f"Comments Scanned: {total_comments} (Last 180 days: {len(recent_comments)})")
        reply.append(f"Newest: {newest}")
        reply.append(f"Eldest: {eldest}\n")
        reply.append("**Unique Comment Activity:**")
        reply.append("Average Inactivity:")
        reply.append(f"All scanned: {avg_gap:.2f} day(s)")
        reply.append(f"Last 180 days: {avg_gap:.2f} day(s)\n")
        reply.append("Maximum Inactivity:")
        reply.append(f"All scanned: {max_gap:.0f} day(s)")
        reply.append(f"Last 180 days: {max_gap:.0f} day(s)\n")
        reply.append("**Daily Activity:**")
        reply.append("Average Inactivity:")
        reply.append(f"All scanned: {avg_day_gap:.2f} day(s)")
        reply.append(f"Last 180 days: {avg_day_gap:.2f} day(s)\n")
        reply.append("Maximum Inactivity:")
        reply.append(f"All scanned: {max_day_gap} day(s)")
        reply.append(f"Last 180 days: {max_day_gap} day(s)\n")
        reply.append("**Comment Karma From (Top 3):**")
        for sr, k in top_3:
            reply.append(f"r/{sr}: {k}")
        reply.append("\n**Account Globals:**")
        reply.append(f"Post Karma: {post_karma}")
        reply.append(f"Comment Karma: {comment_karma}")
        reply.append(f"Combined Karma: {combined}\n")
        reply.append(f"Account Age: {age_days} days ({age_years:.2f} years)")
        reply.append(f"Verified Email: {'Yes' if verified else 'No'}")
        reply.append(f"USL Tags: None")
        
        # Send reply
        comment.reply("\n".join(reply))
        logger.info(f"Stats sent for u/{user}")
    except Exception as e:
        logger.error(f"Error processing stats for u/{user}: {e}")
        logger.error(traceback.format_exc())  # Added consistent error logging
        try:
            comment.reply(f"Error fetching stats for u/{user}.")
        except:
            pass

# Generate user information for posts
def generate_user_info(username):
    conn = get_db_connection()
    if not conn:
        return "Could not retrieve user information due to database error."
    
    try:
        cur = conn.cursor()
        
        # Get user statistics
        cur.execute('''
            SELECT 
                COALESCE(loans_as_borrower, 0) as loans_as_borrower,
                COALESCE(amount_borrowed, 0) as amount_borrowed,
                COALESCE(loans_as_lender, 0) as loans_as_lender,
                COALESCE(amount_lent, 0) as amount_lent,
                COALESCE(amount_repaid, 0) as amount_repaid,
                COALESCE(unpaid_loans, 0) as unpaid_loans,
                COALESCE(unpaid_amount, 0) as unpaid_amount
            FROM users
            WHERE username = %s
        ''', (username.lower(),))
        
        user_stats = cur.fetchone()
        if not user_stats:
            return f"Here is my information on u/{username}:\n\nThis user has no loan history."
        
        loans_as_borrower, amount_borrowed, loans_as_lender, amount_lent, amount_repaid, unpaid_loans, unpaid_amount = user_stats
        
        # Check for unpaid loans where user is borrower
        cur.execute('''
            SELECT COUNT(*) as count
            FROM loans
            WHERE borrower = %s AND (status = 'active' OR status = 'confirmed' OR status = 'partially_repaid')
        ''', (username.lower(),))
        current_unpaid_as_borrower = cur.fetchone()[0]
        
        # Get unpaid loans where user is lender
        cur.execute('''
            SELECT id, borrower, amount, amount_repaid, currency, original_thread
            FROM loans
            WHERE lender = %s AND (status = 'active' OR status = 'confirmed' OR status = 'partially_repaid')
            ORDER BY date_created DESC
            LIMIT 5
        ''', (username.lower(),))
        unpaid_loans_as_lender = cur.fetchall()
        
        # Get in-progress loans where user is lender
        cur.execute('''
            SELECT COUNT(*) as count, SUM(amount - amount_repaid) as total
            FROM loans
            WHERE lender = %s AND (status = 'active' OR status = 'confirmed' OR status = 'partially_repaid')
        ''', (username.lower(),))
        in_progress_count_total = cur.fetchone()
        in_progress_count = in_progress_count_total[0] if in_progress_count_total[0] else 0
        in_progress_total = in_progress_count_total[1] if in_progress_count_total[1] else 0
        
        # Get in-progress loans where user is lender (for display)
        cur.execute('''
            SELECT id, borrower, amount, amount_repaid, currency, original_thread
            FROM loans
            WHERE lender = %s AND (status = 'active' OR status = 'confirmed' OR status = 'partially_repaid')
            ORDER BY date_created DESC
            LIMIT 5
        ''', (username.lower(),))
        in_progress_loans = cur.fetchall()
        
        # Build the response
        response = f"Here is my information on u/{username}:\n\n"
        response += f"**Mobile View**\n\n"
        response += f"u/{username} has {loans_as_borrower} loans paid as a borrower, for a total of ${amount_borrowed:.2f}\n\n"
        response += f"u/{username} has {loans_as_lender} loans paid as a lender, for a total of ${amount_lent:.2f}\n\n"
        
        if current_unpaid_as_borrower == 0:
            response += f"u/{username} has not received any loans which are currently marked unpaid\n\n"
        else:
            response += f"u/{username} has {current_unpaid_as_borrower} current unpaid loans as borrower\n\n"
        
        # Show unpaid loans as lender if any
        if unpaid_loans_as_lender:
            total_unpaid = sum(loan[2] - loan[3] for loan in unpaid_loans_as_lender)
            omitted_count = in_progress_count - len(unpaid_loans_as_lender) if in_progress_count > len(unpaid_loans_as_lender) else 0
            
            response += f"Loans unpaid with u/{username} as lender ({len(unpaid_loans_as_lender)} loans, ${total_unpaid:.2f}) "
            if omitted_count > 0:
                response += f"({omitted_count} loans omitted from the table):\n\n"
            else:
                response += ":\n\n"
            
            response += "Lender | Borrower | Amount Given | Amount Repaid | Unpaid? | Original Thread\n"
            response += "--- | --- | --- | --- | --- | ---\n"
            
            for loan in unpaid_loans_as_lender:
                db_id, borrower, amount, amount_repaid, currency, thread = loan
                response += f"{username.lower()} | {borrower} | {amount:.2f} {currency} | {amount_repaid:.2f} {currency} | UNPAID | {thread}\n"
        
        # Show that user doesn't have outstanding loans as a borrower
        response += f"\nu/{username} does not have any outstanding loans as a borrower\n\n"
        
        # Show in-progress loans as lender if any
        if in_progress_loans:
            omitted_count = in_progress_count - len(in_progress_loans) if in_progress_count > len(in_progress_loans) else 0
            
            response += f"In-progress loans with u/{username} as lender ({in_progress_count} loans, ${in_progress_total:.2f}) "
            if omitted_count > 0:
                response += f"({omitted_count} loans omitted from the table):\n\n"
            else:
                response += ":\n\n"
            
            response += "Lender | Borrower | Amount Given | Amount Repaid | Unpaid? | Original Thread\n"
            response += "--- | --- | --- | --- | --- | ---\n"
            
            for loan in in_progress_loans:
                db_id, borrower, amount, amount_repaid, currency, thread = loan
                response += f"{username.lower()} | {borrower} | {amount:.2f} {currency} | {amount_repaid:.2f} {currency} | {'Yes' if amount_repaid < amount else 'No'} | {thread}\n"
        
        return response
        
    except Exception as e:
        logger.error(f"Error generating user info: {e}")
        logger.error(traceback.format_exc())  # Log full stack trace
        return f"Could not retrieve user information due to an error: {str(e)}"
    finally:
        cur.close()
        conn.close()

# Handle new posts
def handle_new_post(post):
    if post.author is None:
        return
    
    username = post.author.name
    user_info = generate_user_info(username)
    
    try:
        post.reply(user_info)
        logger.info(f"Posted user information for {username}")
    except Exception as e:
        logger.error(f"Error posting user info: {e}")
        logger.error(traceback.format_exc())  # Log full stack trace

# Function to keep the bot alive
def keep_alive():
    while True:
        try:
            logger.info("Keep-alive heartbeat")
            time.sleep(300)  # 5-minute heartbeat
        except Exception as e:
            logger.error(f"Error in keep_alive: {e}")
            logger.error(traceback.format_exc())  # Log full stack trace

# Main bot loop with error handling and reconnection
def comment_monitor():
    while True:
        try:
            subreddit = reddit.subreddit(os.getenv("SUBREDDIT"))
            
            logger.info("Starting comment stream")
            for comment in subreddit.stream.comments(skip_existing=True):
                if comment.author is None or comment.author.name.lower() == os.getenv("REDDIT_USERNAME").lower():
                    continue
                
                body_lower = comment.body.lower()
                
                if "$loan" in body_lower:
                    process_loan_command(comment)
                elif "$confirm" in body_lower:
                    process_confirm_command(comment)
                elif "$paid_with_id" in body_lower:
                    process_paid_command(comment)
                elif "refunded" in body_lower:
                    process_refund_command(comment)
                elif "$stats" in body_lower:
                    process_stats_command(comment)

                    
        except Exception as e:
            logger.error(f"Error in comment stream: {e}")
            logger.error(traceback.format_exc())  # Log full stack trace
            logger.info("Reconnecting in 60 seconds...")
            time.sleep(60)  # Wait 60 seconds before reconnecting

# Post monitor with error handling and reconnection
def post_monitor():
    while True:
        try:
            subreddit = reddit.subreddit(os.getenv("SUBREDDIT"))
            
            logger.info("Starting post stream")
            for post in subreddit.stream.submissions(skip_existing=True):
                # Only process [REQ] posts
                if "[req]" in post.title.lower():
                    handle_new_post(post)
                    
        except Exception as e:
            logger.error(f"Error in post stream: {e}")
            logger.error(traceback.format_exc())  # Log full stack trace
            logger.info("Reconnecting in 60 seconds...")
            time.sleep(60)  # Wait 60 seconds before reconnecting

if __name__ == "__main__":
    if not init_database():
        sys.exit("Failed to initialize database, exiting")

    # Start all threads
    threading.Thread(target=post_monitor, daemon=False).start()  # Changed to non-daemon
    threading.Thread(target=keep_alive, daemon=False).start()    # Added keep-alive thread
    comment_monitor()  # Run the main comment monitor in the main thread
