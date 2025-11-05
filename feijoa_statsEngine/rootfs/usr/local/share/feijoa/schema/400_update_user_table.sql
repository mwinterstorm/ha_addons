DELIMITER //

CREATE TRIGGER update_user_dates_on_contribution
AFTER INSERT ON contributions
FOR EACH ROW
BEGIN
  UPDATE users
  SET
    date_joined = IF(
      date_joined IS NULL OR DATE(NEW.timestamp) < date_joined,
      DATE(NEW.timestamp),
      date_joined
    ),
    last_contrib = IF(
      last_contrib IS NULL OR DATE(NEW.timestamp) > last_contrib,
      DATE(NEW.timestamp),
      last_contrib
    )
  WHERE id = NEW.user_id;
END//

DELIMITER ;