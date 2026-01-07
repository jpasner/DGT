-- Verification Script for CLUE Maryland Data Ingestion
-- Run this after completing the ingestion to verify data quality

\echo '=========================================='
\echo 'CLUE Maryland Data Ingestion Verification'
\echo '=========================================='
\echo ''

-- 1. Check if all tables exist
\echo '1. Checking if all required tables exist...'
SELECT
    CASE
        WHEN COUNT(*) = 5 THEN '✓ All 5 tables exist'
        ELSE '✗ Missing tables! Found: ' || COUNT(*)::text || ' of 5'
    END as status
FROM information_schema.tables
WHERE table_name IN ('source_data', 'case_types', 'final_cases', 'final_defendants', 'final_plaintiffs')
    AND table_schema = 'public';

\echo ''
\echo '2. Checking row counts in all tables...'
-- 2. Row counts
SELECT
    'source_data' as table_name,
    COUNT(*) as row_count,
    CASE
        WHEN COUNT(*) > 25000 THEN '✓'
        WHEN COUNT(*) > 0 THEN '⚠ Less than expected'
        ELSE '✗ Empty!'
    END as status
FROM source_data
UNION ALL
SELECT
    'case_types',
    COUNT(*),
    CASE
        WHEN COUNT(*) > 300 THEN '✓'
        WHEN COUNT(*) > 0 THEN '⚠ Less than expected'
        ELSE '✗ Empty!'
    END
FROM case_types
UNION ALL
SELECT
    'final_cases',
    COUNT(*),
    CASE
        WHEN COUNT(*) > 0 THEN '✓'
        ELSE '✗ Empty!'
    END
FROM final_cases
UNION ALL
SELECT
    'final_defendants',
    COUNT(*),
    CASE
        WHEN COUNT(*) > 0 THEN '✓'
        ELSE '✗ Empty!'
    END
FROM final_defendants
UNION ALL
SELECT
    'final_plaintiffs',
    COUNT(*),
    CASE
        WHEN COUNT(*) > 0 THEN '✓'
        ELSE '✗ Empty!'
    END
FROM final_plaintiffs
ORDER BY table_name;

\echo ''
\echo '3. Checking case categorization distribution...'
-- 3. Category distribution
SELECT
    category,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM final_cases
WHERE category IS NOT NULL
GROUP BY category
ORDER BY count DESC;

\echo ''
\echo '4. Checking for NULL values in key fields...'
-- 4. NULL checks
SELECT
    'Cases with NULL rowID' as check_name,
    COUNT(*) as null_count,
    CASE WHEN COUNT(*) = 0 THEN '✓' ELSE '✗ Fix needed' END as status
FROM final_cases
WHERE rowID IS NULL
UNION ALL
SELECT
    'Cases with NULL category',
    COUNT(*),
    CASE WHEN COUNT(*) < (SELECT COUNT(*) * 0.5 FROM final_cases) THEN '✓' ELSE '⚠ Too many' END
FROM final_cases
WHERE category IS NULL
UNION ALL
SELECT
    'Defendants with NULL rowID',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN '✓' ELSE '✗ Fix needed' END
FROM final_defendants
WHERE rowID IS NULL
UNION ALL
SELECT
    'Plaintiffs with NULL rowID',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN '✓' ELSE '✗ Fix needed' END
FROM final_plaintiffs
WHERE rowID IS NULL;

\echo ''
\echo '5. Sample data from final_cases...'
-- 5. Sample data
SELECT
    rowID,
    caseNumber,
    LEFT(caption, 50) as caption,
    caseType,
    fileDate,
    category
FROM final_cases
WHERE category IS NOT NULL
LIMIT 5;

\echo ''
\echo '6. Checking defendant/plaintiff linkage...'
-- 6. Check joins
SELECT
    'Cases with defendants' as metric,
    COUNT(DISTINCT c.rowID) as count,
    ROUND(100.0 * COUNT(DISTINCT c.rowID) / NULLIF((SELECT COUNT(*) FROM final_cases), 0), 2) as percentage
FROM final_cases c
INNER JOIN final_defendants d ON c.rowID = d.rowID
UNION ALL
SELECT
    'Cases with plaintiffs',
    COUNT(DISTINCT c.rowID),
    ROUND(100.0 * COUNT(DISTINCT c.rowID) / NULLIF((SELECT COUNT(*) FROM final_cases), 0), 2)
FROM final_cases c
INNER JOIN final_plaintiffs p ON c.rowID = p.rowID
UNION ALL
SELECT
    'Cases with both',
    COUNT(DISTINCT c.rowID),
    ROUND(100.0 * COUNT(DISTINCT c.rowID) / NULLIF((SELECT COUNT(*) FROM final_cases), 0), 2)
FROM final_cases c
INNER JOIN final_defendants d ON c.rowID = d.rowID
INNER JOIN final_plaintiffs p ON c.rowID = p.rowID;

\echo ''
\echo '7. Checking for duplicate records...'
-- 7. Duplicate check
SELECT
    'Duplicate case IDs' as check_name,
    COUNT(*) - COUNT(DISTINCT rowID) as duplicate_count,
    CASE WHEN COUNT(*) = COUNT(DISTINCT rowID) THEN '✓ No duplicates' ELSE '⚠ Has duplicates' END as status
FROM final_cases;

\echo ''
\echo '8. Court system distribution...'
-- 8. Court systems
SELECT
    courtSystem,
    COUNT(*) as count
FROM final_cases
WHERE courtSystem IS NOT NULL
GROUP BY courtSystem
ORDER BY count DESC
LIMIT 10;

\echo ''
\echo '=========================================='
\echo 'Verification Complete!'
\echo '=========================================='
\echo ''
\echo 'If you see ✓ marks above, the ingestion was successful.'
\echo 'If you see ✗ marks, review the errors and check the ingestion steps.'
\echo 'If you see ⚠ marks, the data loaded but may need review.'
