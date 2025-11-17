## שלב 1: התקנת תלויות

pip install -r requirements.txt
pip install pymysql
pip install psycopg2-binary

## שלב 2: הגדרת קובץ .env וחיבור ל-DB

DB_DRIVER=mysql+pymysql
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=3306
DB_NAME=your_database_name


**דוגמאות לחיבורי DB מסוגים שונים:**

### MySQL:

DB_DRIVER=mysql+pymysql
DB_USERNAME=root
DB_PASSWORD=mypassword
DB_HOST=localhost
DB_PORT=3306
DB_NAME=shookbook_db


### PostgreSQL:
DB_DRIVER=postgresql+psycopg2
DB_USERNAME=postgres
DB_PASSWORD=mypassword
DB_HOST=localhost
DB_PORT=5432
DB_NAME=shookbook_db


### SQL Server:
DB_DRIVER=mssql+pyodbc
DB_USERNAME=sa
DB_PASSWORD=mypassword
DB_HOST=localhost
DB_PORT=1433
DB_NAME=shookbook_db


## שלב 3: להריץ את הסקריפט בטרמינל

py Shookbook_Reports_By_Date.py
אם לא עובד, לנסות python במקום py

הסקריפט יבקש טווח תאריכים (עם אפשרות להשתמש ב-7 ימים אחרונים כדיפולט) + תאריכי קאטאוף ללקוחות חדשים (אם נדרש)

## שלב 4: תוצאות 
יווצר לנו קובץ Excel בשם `weekly_report.xlsx` עם כל הדוחות מופרדים לגיליונות שונים.

## פתרון בעיות
### שגיאת חיבור למסד נתונים:
- ודא שקובץ `.env` קיים ונכונות הפרטים
- ודא שמסד הנתונים פעיל ונגיש
- ודא שהדרייבר של מסד הנתונים מותקן (pymysql, psycopg2-binary, וכו')

### שגיאת התקנת חבילות:
pip install --upgrade pip
pip install -r requirements.txt


### שגיאת encoding בעברית:
אם יש בעיות עם טקסט עברי, ודא שה-terminal תומך ב-UTF-8.
הסקריפט מנסה להגדיר UTF-8 אוטומטית, אבל אם עדיין יש בעיות:
- ב-Windows Terminal: הגדר encoding ל-UTF-8 בהגדרות
- ב-CMD: הרץ `chcp 65001` לפני הרצת הסקריפט

### אם אנחנו מקבלים רק את המילה "Python" בטרמינל:
קורה כש-`python` הוא alias מה-Windows Store. פשוט להשתמש ב-`py` במקום (py Shookbook_Reports_By_Date.py)

