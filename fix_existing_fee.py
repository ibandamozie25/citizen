
import sqlite3

def fix_existing_fees(db_path='school.db'):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    fees = c.execute("SELECT * FROM fees WHERE payment_type = 'school_fees'").fetchall()

    for fee in fees:
        sid, term, year = fee['student_id'], fee['term'], fee['year']

        # Fetch expected from class fee
        student = c.execute(
            "SELECT class_name, level, boarding_type FROM students WHERE id = ?",
            (sid,)
        ).fetchone()
        if not student:
            continue

        class_fee = c.execute("""
            SELECT amount FROM class_fees
            WHERE class_name = ? AND level = ? AND boarding_status = ?
        """, (student['class_name'], student['level'], student['boarding_type'])).fetchone()
        expected = class_fee['amount'] if class_fee else 0

        # Bursary
        bursary = c.execute("""
            SELECT SUM(amount) as total FROM bursaries
            WHERE student_id = ? AND term = ? AND year = ?
        """, (sid, term, year)).fetchone()
        bursary_amount = bursary['total'] if bursary and bursary['total'] else 0

        # Carry forward (from most recent previous record)
        prev = c.execute("""
            SELECT expected_amount, bursary_amount, amount_paid
            FROM fees
            WHERE student_id = ? AND (year < ? OR (year = ? AND term != ?)) AND payment_type = 'school_fees'
            ORDER BY year DESC, term DESC LIMIT 1
        """, (sid, year, year, term)).fetchone()

        carried = 0
        if prev:
            carried = (prev['expected_amount'] - prev['bursary_amount']) - prev['amount_paid']
            carried = max(carried, 0)

        # Update the fee record
        c.execute("""
            UPDATE fees SET expected_amount = ?, bursary_amount = ?, carried_forward = ?
            WHERE id = ?
        """, (expected, bursary_amount, carried, fee['id']))

        print(f"Updated Student {sid} (Term: {term}, Year: {year}) -> Expected={expected}, Bursary={bursary_amount}, Carry={carried}")

    conn.commit()
    conn.close()
    print("[DONE] All fee records updated.")


if __name__ == '__main__':
    fix_existing_fees()
