-- Reverse 0011: drop the dead_letters table and its indexes.
-- No data-preservation contract here; an operator rolling back this
-- migration is accepting the loss of every queued dead letter.
DROP TABLE IF EXISTS dead_letters;
