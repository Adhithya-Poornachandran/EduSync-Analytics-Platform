# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "99abfbbc-cc0c-43f1-a382-bbf84f43236e",
# META       "default_lakehouse_name": "edusync_Lakehouse",
# META       "default_lakehouse_workspace_id": "e9e3e186-ecf0-42c7-a19a-cc0b2bedf512",
# META       "known_lakehouses": [
# META         {
# META           "id": "99abfbbc-cc0c-43f1-a382-bbf84f43236e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Inspect bronze Data & Check data quality**

# CELL ********************


# Inspect the Bronze tables before cleaning

tables = [
    "bronze_students",
    "bronze_attendance",
    "bronze_marks",
    "bronze_fees",
    "bronze_placements"
]

for table_name in tables:
    print(f"\n--- {table_name} ---")

    df = spark.table(table_name)

    print("Rows:", df.count())
    df.printSchema()
    display(df.limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect bronze_Attendence & Check data quality**

# CELL ********************


from pyspark.sql import functions as F

df = spark.table("bronze_attendance")

# Check missing values
df.select([
    F.count(
        F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)
    ).alias(c)
    for c in df.columns
]).show()

# Check duplicate attendance records
print("Total rows:", df.count())
print("Duplicate rows:", df.count() - df.dropDuplicates().count())

# Check whether IDs are duplicated
print(
    "Duplicate AttendanceIDs:",
    df.count() - df.select("AttendanceID").distinct().count()
)

# Preview actual values
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create The Silver_Table For Attendence**

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read Bronze data and standardize data types
df = (
    spark.table("bronze_attendance")
    .withColumn("AttendanceID", F.trim(F.col("AttendanceID")))
    .withColumn("StudentID", F.trim(F.col("StudentID")))
    .withColumn(
        "WeekStartDate",
        F.to_date(F.col("WeekStartDate"))
    )
    .withColumn("ClassesHeld", F.col("ClassesHeld").cast("int"))
    .withColumn("ClassesAttended", F.col("ClassesAttended").cast("int"))
)

# 2. Add data-quality validation
df = df.withColumn(
    "DataQualityStatus",
    F.when(
        F.col("AttendanceID").isNull() |
        (F.col("AttendanceID") == ""),
        "REJECTED_MISSING_ATTENDANCE_ID"
    )
    .when(
        F.col("StudentID").isNull() |
        (F.col("StudentID") == ""),
        "REJECTED_MISSING_STUDENT_ID"
    )
    .when(
        F.col("WeekStartDate").isNull(),
        "REJECTED_INVALID_DATE"
    )
    .when(
        F.col("ClassesHeld").isNull() |
        F.col("ClassesAttended").isNull(),
        "REJECTED_INVALID_CLASS_COUNT"
    )
    .when(
        (F.col("ClassesHeld") <= 0) |
        (F.col("ClassesAttended") < 0) |
        (F.col("ClassesAttended") > F.col("ClassesHeld")),
        "REJECTED_INVALID_ATTENDANCE"
    )
    .otherwise("VALID")
)

# 3. Deduplicate by AttendanceID
# Prefer valid records when duplicate IDs exist.
window_spec = Window.partitionBy("AttendanceID").orderBy(
    F.when(F.col("DataQualityStatus") == "VALID", 1).otherwise(0).desc(),
    F.col("WeekStartDate").desc_nulls_last()
)

df = (
    df
    .withColumn("_row_num", F.row_number().over(window_spec))
    .withColumn(
        "DataQualityStatus",
        F.when(
            F.col("_row_num") > 1,
            "REJECTED_DUPLICATE_ATTENDANCE_ID"
        ).otherwise(F.col("DataQualityStatus"))
    )
    .drop("_row_num")
)

# 4. Create computed columns
df = (
    df
    .withColumn(
        "AttendancePercentage",
        F.when(
            F.col("ClassesHeld") > 0,
            F.round(
                F.col("ClassesAttended") / F.col("ClassesHeld") * 100,
                2
            )
        )
    )
    .withColumn(
        "ClassesMissed",
        F.when(
            F.col("ClassesHeld").isNotNull() &
            F.col("ClassesAttended").isNotNull(),
            F.col("ClassesHeld") - F.col("ClassesAttended")
        )
    )
    .withColumn(
        "AttendanceStatus",
        F.when(
            F.col("AttendancePercentage").isNull(),
            "UNKNOWN"
        )
        .when(F.col("AttendancePercentage") >= 90, "GOOD")
        .when(F.col("AttendancePercentage") >= 75, "WARNING")
        .otherwise("CRITICAL")
    )
)

# 5. Split accepted and rejected records
silver_attendance = df.filter(
    F.col("DataQualityStatus") == "VALID"
)

silver_attendance_rejected = df.filter(
    F.col("DataQualityStatus") != "VALID"
)

# 6. Save both tables in Silver_Schema
(
    silver_attendance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_attendance")
)

(
    silver_attendance_rejected.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_attendance_rejected")
)

print("Accepted rows:", silver_attendance.count())
print("Rejected rows:", silver_attendance_rejected.count())

display(silver_attendance.limit(10))
display(silver_attendance_rejected.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect bronze_students & Check data quality**

# CELL ********************


from pyspark.sql import functions as F

df = spark.table("bronze_students")

print("Total rows:", df.count())
df.printSchema()
display(df.limit(10))


# Count missing or blank values in each column
df.select([
    F.count(
        F.when(
            F.col(c).isNull() | (F.trim(F.col(c)) == ""),
            c
        )
    ).alias(c)
    for c in df.columns
]).show()

# Check exact duplicate rows
print("Exact duplicate rows:", df.count() - df.dropDuplicates().count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Bronze to Silver transformation for Silver_Students**

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read Bronze and standardize values
df = spark.table("bronze_students")

df = (
    df
    .withColumn("StudentID", F.trim(F.col("StudentID")))
    .withColumn("FirstName", F.initcap(F.trim(F.col("FirstName"))))
    .withColumn("LastName", F.initcap(F.trim(F.col("LastName"))))
    .withColumn("Department", F.initcap(F.trim(F.col("Department"))))
    .withColumn("Email", F.lower(F.trim(F.col("Email"))))
    .withColumn("JoinYear", F.col("JoinYear").cast("int"))
)

# Convert blank strings to NULL
for c in ["StudentID", "FirstName", "LastName", "Department", "Email"]:
    df = df.withColumn(
        c,
        F.when(F.col(c) == "", None).otherwise(F.col(c))
    )

# 2. Create computed columns
df = (
    df
    .withColumn(
        "FullName",
        F.concat_ws(" ", F.col("FirstName"), F.col("LastName"))
    )
    .withColumn(
        "EmailDomain",
        F.when(
            F.col("Email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
            F.element_at(F.split(F.col("Email"), "@"), -1)
        )
    )
    .withColumn(
        "StudentTenureYears",
        F.year(F.current_date()) - F.col("JoinYear")
    )
)

# 3. Flag data quality issues
current_year = F.year(F.current_date())

df = (
    df
    .withColumn(
        "EmailValid",
        F.coalesce(
            F.col("Email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
            F.lit(False)
        )
    )
    .withColumn(
        "JoinYearValid",
        F.coalesce(
            (F.col("JoinYear") >= 2000) &
            (F.col("JoinYear") <= current_year),
            F.lit(False)
        )
    )
    .withColumn(
        "DataQualityStatus",
        F.when(
            F.col("StudentID").isNull(),
            "REJECTED_MISSING_ID"
        )
        .when(
            F.col("FirstName").isNull() |
            F.col("LastName").isNull(),
            "REVIEW_MISSING_NAME"
        )
        .when(~F.col("EmailValid"), "REVIEW_INVALID_EMAIL")
        .when(~F.col("JoinYearValid"), "REVIEW_INVALID_JOIN_YEAR")
        .otherwise("VALID")
    )
)

# 4. Deduplicate by StudentID
# Prefer the record with more complete fields.
# No timestamp exists in this source, so this is NOT latest-record logic.
df = df.withColumn(
    "_CompletenessScore",
    F.when(F.col("FirstName").isNotNull(), 1).otherwise(0) +
    F.when(F.col("LastName").isNotNull(), 1).otherwise(0) +
    F.when(F.col("Department").isNotNull(), 1).otherwise(0) +
    F.when(F.col("EmailValid"), 1).otherwise(0) +
    F.when(F.col("JoinYearValid"), 1).otherwise(0)
)

window_spec = Window.partitionBy("StudentID").orderBy(
    F.col("_CompletenessScore").desc(),
    F.col("EmailValid").desc(),
    F.col("JoinYearValid").desc()
)

df = (
    df
    .withColumn("_row_num", F.row_number().over(window_spec))
    .withColumn(
        "DataQualityStatus",
        F.when(
            F.col("StudentID").isNull(),
            "REJECTED_MISSING_ID"
        )
        .when(F.col("_row_num") > 1, "REJECTED_DUPLICATE_ID")
        .otherwise(F.col("DataQualityStatus"))
    )
)

# 5. Separate accepted and rejected records
silver_students = (
    df.filter(F.col("DataQualityStatus") == "VALID")
    .drop("_CompletenessScore", "_row_num")
)

silver_students_rejected = (
    df.filter(F.col("DataQualityStatus") != "VALID")
    .drop("_CompletenessScore", "_row_num")
)

# 6. Save both tables in the Silver schema
(
    silver_students.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_students")
)

(
    silver_students_rejected.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_students_rejected")
)

print("Accepted records:", silver_students.count())
print("Rejected/review records:", silver_students_rejected.count())

display(silver_students.limit(10))
display(silver_students_rejected.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect bronze_Marks & Check data quality**

# CELL ********************


from pyspark.sql import functions as F

# Read Bronze Marks
df = spark.table("bronze_marks")

# Basic checks
print("Total rows:", df.count())
print("Exact duplicate rows:", df.count() - df.dropDuplicates().count())

# Inspect column names and data types
df.printSchema()

# Preview sample records
display(df.limit(10))

# Check null and blank values
df.select([
    F.count(
        F.when(
            F.col(c).isNull() |
            (F.trim(F.col(c).cast("string")) == ""),
            c
        )
    ).alias(c)
    for c in df.columns
]).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Bronze to Silver transformation for Silver_Marks**

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read Bronze data and standardize columns
df = (
    spark.table("bronze_marks")
    .withColumn("MarkID", F.trim(F.col("MarkID")))
    .withColumn("StudentID", F.trim(F.col("StudentID")))
    .withColumn("Subject", F.initcap(F.trim(F.col("Subject"))))
    .withColumn("ExamType", F.initcap(F.trim(F.col("ExamType"))))
    .withColumn("MaxMarks", F.col("MaxMarks").cast("double"))
    .withColumn("MarksObtained", F.col("MarksObtained").cast("double"))
)

# Convert blank strings to NULL
for c in ["MarkID", "StudentID", "Subject", "ExamType"]:
    df = df.withColumn(
        c,
        F.when(F.col(c) == "", None).otherwise(F.col(c))
    )

# 2. Apply data-quality rules
df = df.withColumn(
    "DataQualityStatus",
    F.when(
        F.col("MarkID").isNull(),
        "REJECTED_MISSING_MARK_ID"
    )
    .when(
        F.col("StudentID").isNull(),
        "REJECTED_MISSING_STUDENT_ID"
    )
    .when(
        F.col("Subject").isNull() | F.col("ExamType").isNull(),
        "REJECTED_MISSING_SUBJECT_OR_EXAM"
    )
    .when(
        F.col("MaxMarks").isNull() |
        F.col("MarksObtained").isNull(),
        "REJECTED_INVALID_MARK_VALUE"
    )
    .when(
        (F.col("MaxMarks") <= 0) |
        (F.col("MarksObtained") < 0) |
        (F.col("MarksObtained") > F.col("MaxMarks")),
        "REJECTED_OUT_OF_RANGE_MARKS"
    )
    .otherwise("VALID")
)

# 3. Deduplicate by MarkID
# Prefer valid records when duplicate IDs exist.
window_spec = Window.partitionBy("MarkID").orderBy(
    F.when(F.col("DataQualityStatus") == "VALID", 1).otherwise(0).desc(),
    F.col("StudentID").asc_nulls_last(),
    F.col("Subject").asc_nulls_last()
)

df = (
    df
    .withColumn("_row_num", F.row_number().over(window_spec))
    .withColumn(
        "DataQualityStatus",
        F.when(
            F.col("_row_num") > 1,
            "REJECTED_DUPLICATE_MARK_ID"
        ).otherwise(F.col("DataQualityStatus"))
    )
    .drop("_row_num")
)

# 4. Create computed columns
df = (
    df
    .withColumn(
        "Percentage",
        F.when(
            F.col("MaxMarks") > 0,
            F.round(F.col("MarksObtained") / F.col("MaxMarks") * 100, 2)
        )
    )
    .withColumn(
        "Grade",
        F.when(F.col("Percentage").isNull(), "UNKNOWN")
        .when(F.col("Percentage") >= 90, "A")
        .when(F.col("Percentage") >= 80, "B")
        .when(F.col("Percentage") >= 70, "C")
        .when(F.col("Percentage") >= 60, "D")
        .when(F.col("Percentage") >= 50, "E")
        .otherwise("F")
    )
    .withColumn(
        "Result",
        F.when(F.col("Percentage").isNull(), "UNKNOWN")
        .when(F.col("Percentage") >= 40, "PASS")
        .otherwise("FAIL")
    )
)

# 5. Separate accepted and rejected records
silver_marks = df.filter(
    F.col("DataQualityStatus") == "VALID"
)

silver_marks_rejected = df.filter(
    F.col("DataQualityStatus") != "VALID"
)

# 6. Save accepted records
(
    silver_marks.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_marks")
)

# 7. Save rejected records for review
(
    silver_marks_rejected.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_marks_rejected")
)

# 8. Validate the outputs
print("Bronze rows:", spark.table("bronze_marks").count())
print("Accepted rows:", silver_marks.count())
print("Rejected rows:", silver_marks_rejected.count())

display(silver_marks.limit(10))
display(silver_marks_rejected.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect bronze_fees and checks missing Values**

# CELL ********************


from pyspark.sql import functions as F

df = spark.table("bronze_fees")

print("Total rows:", df.count())
print("Exact duplicate rows:", df.count() - df.dropDuplicates().count())

df.printSchema()
display(df.limit(10))


df.select([
    F.count(
        F.when(
            F.col(c).isNull() |
            (F.trim(F.col(c).cast("string")) == ""),
            c
        )
    ).alias(c)
    for c in df.columns
]).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Bronze to Silver transformation for Silver_Marks**

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read Bronze and convert data types
df = (
    spark.table("bronze_fees")
    .withColumn("FeeRecordID", F.trim(F.col("FeeRecordID")))
    .withColumn("StudentID", F.trim(F.col("StudentID")))
    .withColumn("AcademicYear", F.trim(F.col("AcademicYear")))
    .withColumn("TotalFees", F.col("TotalFees").cast("decimal(12,2)"))
    .withColumn("AmountPaid", F.col("AmountPaid").cast("decimal(12,2)"))
    .withColumn("DueDate", F.to_date(F.col("DueDate")))
)

# 2. Convert blank identifiers to NULL
for c in ["FeeRecordID", "StudentID", "AcademicYear"]:
    df = df.withColumn(
        c,
        F.when(F.col(c) == "", None).otherwise(F.col(c))
    )

# 3. Validate fee records
df = df.withColumn(
    "DataQualityStatus",
    F.when(
        F.col("FeeRecordID").isNull(),
        "REJECTED_MISSING_FEE_ID"
    )
    .when(
        F.col("StudentID").isNull(),
        "REJECTED_MISSING_STUDENT_ID"
    )
    .when(
        F.col("AcademicYear").isNull(),
        "REJECTED_MISSING_ACADEMIC_YEAR"
    )
    .when(
        F.col("TotalFees").isNull() | F.col("AmountPaid").isNull(),
        "REJECTED_INVALID_AMOUNT"
    )
    .when(
        (F.col("TotalFees") < 0) | (F.col("AmountPaid") < 0),
        "REJECTED_NEGATIVE_AMOUNT"
    )
    .when(
        F.col("DueDate").isNull(),
        "REJECTED_INVALID_DUE_DATE"
    )
    .otherwise("VALID")
)

# 4. Deduplicate FeeRecordID
window_spec = Window.partitionBy("FeeRecordID").orderBy(
    F.when(F.col("DataQualityStatus") == "VALID", 1).otherwise(0).desc(),
    F.col("DueDate").desc_nulls_last(),
    F.col("StudentID").asc_nulls_last()
)

df = (
    df
    .withColumn("_row_num", F.row_number().over(window_spec))
    .withColumn(
        "DataQualityStatus",
        F.when(
            F.col("_row_num") > 1,
            "REJECTED_DUPLICATE_FEE_ID"
        ).otherwise(F.col("DataQualityStatus"))
    )
    .drop("_row_num")
)

# 5. Create computed business columns
df = (
    df
    .withColumn(
        "OutstandingBalance",
        F.col("TotalFees") - F.col("AmountPaid")
    )
    .withColumn(
        "PaymentStatus",
        F.when(
            F.col("OutstandingBalance") < 0,
            "OVERPAID_REVIEW"
        )
        .when(F.col("OutstandingBalance") == 0, "PAID")
        .otherwise("PARTIALLY_PAID_OR_UNPAID")
    )
    .withColumn(
        "DueStatus",
        F.when(F.col("DueDate").isNull(), "UNKNOWN")
        .when(
            (F.col("OutstandingBalance") > 0) &
            (F.col("DueDate") < F.current_date()),
            "OVERDUE"
        )
        .when(
            F.col("OutstandingBalance") > 0,
            "PENDING"
        )
        .otherwise("NO_OUTSTANDING_BALANCE")
    )
)

# 6. Separate valid and rejected records
silver_fees = df.filter(F.col("DataQualityStatus") == "VALID")
silver_fees_rejected = df.filter(F.col("DataQualityStatus") != "VALID")

# 7. Save Silver tables
(
    silver_fees.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_fees")
)

(
    silver_fees_rejected.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_fees_rejected")
)

# 8. Validate
print("Bronze rows:", spark.table("bronze_fees").count())
print("Accepted rows:", silver_fees.count())
print("Rejected rows:", silver_fees_rejected.count())

display(silver_fees.limit(10))
display(silver_fees_rejected.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Inspect bronze_placements & Check missing values**

# CELL ********************


from pyspark.sql import functions as F

df = spark.table("bronze_placements")

print("Total rows:", df.count())
print("Exact duplicate rows:", df.count() - df.dropDuplicates().count())

df.printSchema()
display(df.limit(10))


df.select([
    F.count(
        F.when(
            F.col(c).isNull() |
            (F.trim(F.col(c).cast("string")) == ""),
            c
        )
    ).alias(c)
    for c in df.columns
]).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Bronze to Silver transformation for Silver_placements**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read Bronze and standardize columns
df = (
    spark.table("bronze_placements")
    .withColumn("PlacementRecordID", F.trim(F.col("PlacementRecordID")))
    .withColumn("StudentID", F.trim(F.col("StudentID")))
    .withColumn("Company", F.trim(F.col("Company")))
    .withColumn("Status", F.upper(F.trim(F.col("Status"))))
    .withColumn("PackageLPA", F.col("PackageLPA").cast("double"))
    .withColumn("UpdatedDate", F.to_date(F.col("UpdatedDate")))
)

# 2. Convert blank strings to NULL
for c in ["PlacementRecordID", "StudentID", "Company", "Status"]:
    df = df.withColumn(
        c,
        F.when(F.col(c) == "", None).otherwise(F.col(c))
    )

# 3. Standardize status values
df = df.withColumn(
    "Status",
    F.when(
        F.col("Status").isin("PLACED", "SELECTED", "OFFERED"),
        "PLACED"
    )
    .when(
        F.col("Status").isin("NOT PLACED", "NOT_PLACED", "UNPLACED"),
        "NOT PLACED"
    )
    .when(F.col("Status") == "NOT ELIGIBLE", "NOT ELIGIBLE")
    .when(F.col("Status") == "ELIGIBLE", "ELIGIBLE")
    .when(F.col("Status") == "APPLIED", "APPLIED")
    .when(F.col("Status") == "INTERVIEW", "INTERVIEW")
    .otherwise(F.col("Status"))
)

# 4. Validate records
df = df.withColumn(
    "DataQualityStatus",
    F.when(
        F.col("PlacementRecordID").isNull(),
        "REJECTED_MISSING_PLACEMENT_ID"
    )
    .when(
        F.col("StudentID").isNull(),
        "REJECTED_MISSING_STUDENT_ID"
    )
    .when(
        F.col("Status").isNull(),
        "REJECTED_MISSING_STATUS"
    )
    .when(
        ~F.col("Status").isin(
            "ELIGIBLE",
            "NOT ELIGIBLE",
            "APPLIED",
            "INTERVIEW",
            "PLACED",
            "NOT PLACED"
        ),
        "REJECTED_UNKNOWN_STATUS"
    )
    .when(
        (F.col("Status") == "PLACED") &
        (
            F.col("Company").isNull() |
            F.col("PackageLPA").isNull() |
            (F.col("PackageLPA") <= 0)
        ),
        "REJECTED_INVALID_PLACEMENT_DETAILS"
    )
    .when(
        F.col("PackageLPA").isNotNull() &
        (F.col("PackageLPA") < 0),
        "REJECTED_INVALID_PACKAGE"
    )
    .when(
        F.col("UpdatedDate").isNull(),
        "REJECTED_INVALID_UPDATED_DATE"
    )
    .otherwise("VALID")
)

# 5. Deduplicate by PlacementRecordID
window_spec = Window.partitionBy("PlacementRecordID").orderBy(
    F.when(F.col("DataQualityStatus") == "VALID", 1)
     .otherwise(0).desc(),
    F.col("UpdatedDate").desc_nulls_last(),
    F.col("StudentID").asc_nulls_last()
)

df = (
    df
    .withColumn("_row_num", F.row_number().over(window_spec))
    .withColumn(
        "DataQualityStatus",
        F.when(
            F.col("_row_num") > 1,
            "REJECTED_DUPLICATE_PLACEMENT_ID"
        ).otherwise(F.col("DataQualityStatus"))
    )
    .drop("_row_num")
)

# 6. Create business columns
df = (
    df
    .withColumn(
        "PlacementFlag",
        F.when(F.col("Status") == "PLACED", 1).otherwise(0)
    )
    .withColumn(
        "PlacementStage",
        F.when(F.col("Status") == "PLACED", "PLACED")
         .when(F.col("Status") == "NOT PLACED", "CLOSED_NOT_PLACED")
         .when(F.col("Status") == "NOT ELIGIBLE", "NOT_ELIGIBLE")
         .when(F.col("Status") == "ELIGIBLE", "ELIGIBLE")
         .when(F.col("Status") == "APPLIED", "APPLICATION")
         .when(F.col("Status") == "INTERVIEW", "INTERVIEW")
         .otherwise("UNKNOWN")
    )
    .withColumn(
        "PackageCategory",
        F.when(
            F.col("Status") != "PLACED",
            "NOT_APPLICABLE"
        )
        .when(F.col("PackageLPA") >= 10, "HIGH")
        .when(F.col("PackageLPA") >= 5, "MEDIUM")
        .when(F.col("PackageLPA") > 0, "ENTRY_LEVEL")
        .otherwise("UNKNOWN")
    )
    .withColumn("UpdateYear", F.year(F.col("UpdatedDate")))
)

# 7. Split accepted and rejected records
silver_placements = df.filter(
    F.col("DataQualityStatus") == "VALID"
)

silver_placements_rejected = df.filter(
    F.col("DataQualityStatus") != "VALID"
)

# 8. Save Silver tables
(
    silver_placements.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_placements")
)

(
    silver_placements_rejected.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("Silver_Schema.silver_placements_rejected")
)

# 9. Validate output
print("Bronze rows:", spark.table("bronze_placements").count())
print("Accepted rows:", spark.table("Silver_Schema.silver_placements").count())
print("Rejected rows:", spark.table("Silver_Schema.silver_placements_rejected").count())

# Check accepted status distribution
display(
    spark.table("Silver_Schema.silver_placements")
    .groupBy("Status")
    .count()
    .orderBy("Status")
)

# Check rejection reasons
display(
    spark.table("Silver_Schema.silver_placements_rejected")
    .groupBy("DataQualityStatus")
    .count()
    .orderBy(F.desc("count"))
)

display(spark.table("Silver_Schema.silver_placements").limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **audit all Silver tables**

# CELL ********************

tables = [
    "Silver_Schema.silver_students",
    "Silver_Schema.silver_attendance",
    "Silver_Schema.silver_marks",
    "Silver_Schema.silver_fees",
    "Silver_Schema.silver_placements",
    "Silver_Schema.silver_students_rejected",
    "Silver_Schema.silver_attendance_rejected",
    "Silver_Schema.silver_marks_rejected",
    "Silver_Schema.silver_fees_rejected",
    "Silver_Schema.silver_placements_rejected"
]

for table_name in tables:
    df = spark.table(table_name)
    print(f"{table_name}: {df.count()} rows")
    print(f"{table_name}: {df.printSchema()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ****
