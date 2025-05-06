# Database Schema for LoanCentral

## Tables

### loans
Stores information about individual loans

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key, auto-incrementing |
| loan_id | TEXT | Unique identifier for the loan (used in commands) |
| lender | TEXT | Username of the lender |
| borrower | TEXT | Username of the borrower |
| amount | NUMERIC | Amount of the loan |
| currency | TEXT | Currency of the loan (USD, CAD, etc.) |
| date_created | TIMESTAMP | When the loan was created |
| original_thread | TEXT | URL to the original thread |
| status | TEXT | Status of the loan (active, confirmed, partially_repaid, repaid, refunded) |
| amount_repaid | NUMERIC | Amount that has been repaid |
| last_updated | TIMESTAMP | When the loan was last updated |

### users
Stores aggregate statistics about users

| Column | Type | Description |
|--------|------|-------------|
| username | TEXT | Primary key, username |
| loans_as_borrower | INTEGER | Number of loans taken as a borrower |
| loans_as_lender | INTEGER | Number of loans given as a lender |
| amount_borrowed | NUMERIC | Total amount borrowed |
| amount_lent | NUMERIC | Total amount lent |
| amount_repaid | NUMERIC | Total amount repaid |
| unpaid_loans | INTEGER | Number of unpaid loans |
| unpaid_amount | NUMERIC | Total amount of unpaid loans |
| last_updated | TIMESTAMP | When the user record was last updated |

## Indexes

```sql
CREATE INDEX idx_loans_lender ON loans(lender);
CREATE INDEX idx_loans_borrower ON loans(borrower);
CREATE INDEX idx_loans_status ON loans(status);
CREATE INDEX idx_loans_date_created ON loans(date_created);
```

## Sample Queries

### Get user loan history
```sql
SELECT * FROM loans WHERE lender = 'username' OR borrower = 'username' ORDER BY date_created DESC;
```

### Get unpaid loans
```sql
SELECT * FROM loans WHERE status IN ('active', 'confirmed', 'partially_repaid') ORDER BY date_created;
```

### Get user statistics
```sql
SELECT * FROM users WHERE username = 'username';
```