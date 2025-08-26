# CRM Introduction 

Read this in order to use CRM utils to export joist data into CreativeFloors local database
Until Joist is going to implement an api data export is done manually:

1. Estimates - incremental, by month
2. Clients - full always
3. Invoices - incremental, by month

---

## 1. Order of export

Export is done manually for each of the 3 types of data objects in joist. The order does not matter

## 2. Order of import/ingestion

Due to the lack of entry data validation, data usually contains a lot of mistakes and duplicates. 
Estimates and Invoices are exported by month. Invoices use tab as separator.


## 3. How to run the ingestor [incremental]

Incremental means that the estimator expects the database and tables to exist and contain some data.
1. all_ingestor.py      --> main: to be executed
2. ingest_data.py       --> util functions called by the all_ingestor.py

The order of execution in main is, 
1. Clients [uses estimates to add to clients only those who have an estimate]
2. Estimates [requires the full_name to already exist in clients table: FK]
3. Invoices  [requires the full_name to already exist in clients table: FK]

### 3.1 Execution 
Before executing the script, make sure the 3 csv files are present in /data/{folders}
folders: clients, estimates, invoices with the corresponding names

Configure the c_month variable with the month to be ingested and the date of the execution: ingestion_date_str

Logger might work, in logs/old you have sample logs for the full ingester 

### 3.2 Cleaner for string columns

For all the entities, there is a method clean_df_cols that receives 2 parameters and returns a clean dataframe.
First: dataframe to be cleaned
Second: a python list of columns to be cleaned, each element of the list is a string. 
This was configured for the column full_name of estimates and invoices and for all the rest of the string columns of clients. 

This method replaces problematic characters with space, also strips the spaces before the value and after the value.
It does happen, and that's the reason the strip is there. 

## 4. How to run the ingestor full [first time]
##### Pending review and documentation
###### H6 Heading

---

## 2. Text Formatting

This is **bold text** using two asterisks.
This is **bold text** using two underscores.

This is *italic text* using a single asterisk.
This is *italic text* using a single underscore.

This is ***bold and italic text***.
This is ***bold and italic text*** using both.

This is ~~strikethrough text~~.

This is a paragraph with a `code snippet` inline.

> This is a blockquote.
> It can span multiple lines.

---

## 3. Lists

### Unordered List

* Item 1
* Item 2
    * Sub-item 2.1
    * Sub-item 2.2
* Item 3

### Ordered List

1. First item
2. Second item
3. Third item
    1. A sub-list item
    2. Another sub-list item

### Task List

- [x] Task 1 (completed)
- [ ] Task 2 (not completed)
- [ ] Task 3

---

## 4. Links and Images

This is a [link to Google](https://www.google.com).

This is a link to a file in the same directory: [Local File Link](local-file.md).

This is an image:
![A cute dog](https://placedog.net/500/280)

This is a link and an image combined:
[![A cute dog](https://placedog.net/300/170)](https://placedog.net)

---

## 5. Code Blocks

### Inline Code

Here is some `inline code` for a simple function.

### Fenced Code Block

```python
def hello_world():
  print("Hello, World!")

# You can specify the language for syntax highlighting