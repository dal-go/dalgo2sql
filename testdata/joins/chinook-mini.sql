CREATE TABLE Invoice (
  InvoiceId INTEGER PRIMARY KEY,
  CustomerId INTEGER NOT NULL,
  Total REAL NOT NULL
);
CREATE TABLE Customer (
  CustomerId INTEGER PRIMARY KEY,
  FirstName TEXT NOT NULL,
  SupportRepId INTEGER
);
CREATE TABLE Employee (
  EmployeeId INTEGER PRIMARY KEY,
  FirstName TEXT NOT NULL
);

INSERT INTO Customer (CustomerId, FirstName, SupportRepId) VALUES
  (10, 'Ada', 100),
  (20, 'Bea', NULL);
INSERT INTO Employee (EmployeeId, FirstName) VALUES (100, 'Evan');
INSERT INTO Invoice (InvoiceId, CustomerId, Total) VALUES
  (1, 10, 12.5),
  (2, 10, 8.0),
  (3, 20, 4.0);
