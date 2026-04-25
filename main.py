import pandas as pd
import sqlite3

# Connections
conn1 = sqlite3.connect('planets.db')
conn2 = sqlite3.connect('dogs.db')
conn3 = sqlite3.connect('babe_ruth.db')

# ── Part 1: Basic Filtering ───────────────────────────────────────────────────

# Step 1: All columns for planets with 0 moons
df_no_moons = pd.read_sql("""
SELECT *
FROM planets
WHERE num_of_moons = 0;
""", conn1)

# Step 2: Name and mass of planets with exactly 7-letter names
df_7_letters = pd.read_sql("""
SELECT name, mass
FROM planets
WHERE LENGTH(name) = 7;
""", conn1)

# ── Part 2: Advanced Filtering ────────────────────────────────────────────────

# Step 3: Name and mass for planets with mass <= 1.00
df_mass = pd.read_sql("""
SELECT name, mass
FROM planets
WHERE mass <= 1.00;
""", conn1)

# Step 4: All columns for planets with at least one moon AND mass < 1.00
df_moons_and_mass = pd.read_sql("""
SELECT *
FROM planets
WHERE num_of_moons >= 1
  AND mass < 1.00;
""", conn1)

# Step 5: Name and color for planets whose color contains "blue"
df_blue = pd.read_sql("""
SELECT name, color
FROM planets
WHERE color LIKE '%blue%';
""", conn1)

# ── Part 3: Ordering and Limiting ─────────────────────────────────────────────

# Step 6: Hungry dogs sorted youngest to oldest
df_hungry_dogs = pd.read_sql("""
SELECT name, age, breed
FROM dogs
WHERE hungry = 1
ORDER BY age ASC;
""", conn2)

# Step 7: Hungry dogs aged 2-7, sorted alphabetically by name
df_hungry_young = pd.read_sql("""
SELECT name, age, hungry
FROM dogs
WHERE hungry = 1
  AND age BETWEEN 2 AND 7
ORDER BY name ASC;
""", conn2)

# Step 8: The 4 oldest dogs, sorted alphabetically by breed
df_oldest = pd.read_sql("""
SELECT name, age, breed
FROM (
    SELECT name, age, breed
    FROM dogs
    ORDER BY age DESC
    LIMIT 4
)
ORDER BY breed ASC;
""", conn2)

# ── Part 4: Aggregation ───────────────────────────────────────────────────────

# Step 9: Total number of years Babe Ruth played
df_years_played = pd.read_sql("""
SELECT COUNT(year) AS total_years
FROM babe_ruth_stats;
""", conn3)

# Step 10: Total career home runs
df_hr_total = pd.read_sql("""
SELECT SUM(HR) AS total_home_runs
FROM babe_ruth_stats;
""", conn3)

# ── Part 5: Grouping and Aggregation ──────────────────────────────────────────

# Step 11: Team name and number of years played per team
df_years_per_team = pd.read_sql("""
SELECT team,
       COUNT(year) AS number_years
FROM babe_ruth_stats
GROUP BY team;
""", conn3)

# Step 12: Teams where Babe Ruth averaged over 200 at bats
df_avg_at_bats = pd.read_sql("""
SELECT team,
       AVG(at_bats) AS average_at_bats
FROM babe_ruth_stats
GROUP BY team
HAVING AVG(at_bats) > 200;
""", conn3)

# ── Close connections ─────────────────────────────────────────────────────────
conn1.close()
conn2.close()
conn3.close()
