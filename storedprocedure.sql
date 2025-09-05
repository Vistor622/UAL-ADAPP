DELIMITER //

CREATE PROCEDURE sp_BulkInsertImport_file_mysql_27177(IN col_names TEXT, IN col_values TEXT)
BEGIN
    SET @sql = CONCAT('INSERT INTO Import (', col_names, ') VALUES ', col_values, ';');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;