-- One database per service: no service reads another's tables.
CREATE DATABASE orders;
CREATE DATABASE payments;
CREATE DATABASE inventory;
