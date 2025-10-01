
import sqlite3

def sync_bursaries_to_fees(db_path='school.db'):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    bursaries = c.execute("SELECT * FROM bursaries").fetchall()
    count_updated = 0
    count_inserted = 0

    for bursary in bursaries:
        student_id = bursary['student_id']
        term = bursary['term']
        year = bursary['year']
        bursary_amount = bursary['amount']

        fee = c.execute("""
            SELECT * FROM fees
            WHERE student_id = ? AND term = ? AND year = ? AND payment_type = 'school_fees'
        """, (student_id, term, year)).fetchone()

        if fee:
            # Update bursary_amount in existing fee record
            c.execute("""
                UPDATE fees
                SET bursary_amount = ?
                WHERE id = ?
            """, (bursary_amount, fee['id']))
            count_updated += 1
        else:
            # Try to find expected fee based on class fee
            student = c.execute("""
                SELECT class_name, level, boarding_type 
                FROM students 
                WHERE id = ?
            """, (student_id,)).fetchone()

            if not student:
                continue

            class_fee = c.execute("""
                SELECT amount FROM class_fees
                WHERE class_name = ? AND level = ? AND boarding_status = ?
            """, (student['class_name'], student['level'], student['boarding_type'])).fetchone()

            expected = class_fee['amount'] if class_fee else 0

            c.execute("""
                INSERT INTO fees (
                    student_id, term, year, expected_amount, bursary_amount,
                    carried_forward, amount_paid, date_paid, method, payment_type
                ) VALUES (?, ?, ?, ?, ?, 0, 0, NULL, 'N/A', 'school_fees')
            """, (student_id, term, year, expected, bursary_amount))
            count_inserted += 1

    conn.commit()
    conn.close()

    print(f"Updated bursaries on existing fees: {count_updated}")
    print(f"Inserted new fees with bursaries: {count_inserted}")

# Run this file manually to sync
if __name__ == '__main__':
    sync_bursaries_to_fees()
