# Utility package for database upload/ download/ execute

## Status
- Minimal Viable Product
- used in production by a big corporate company for more than 4 year as of now

## Features
- .xlsx/ .csv/ .txt/ .json streaming upload/ download
- executing SQL queries with(out) parameters
- returning db errors/  messages
- for big data uploads, returns error row numbers and specific db error message
- utility classes for GUI tables with dynamic filtering, sorting, pagination and data update
- callbacks before/ after execution/ upload
- Azure DevOps CI/CD

## Dependecies
- JDBC
- Apache POI
- XLSX Streamer