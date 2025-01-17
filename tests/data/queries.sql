-- name: GetHolelocations :many
SELECT * FROM HOLELOCATION;

-- name: GetEquipments :many
SELECT * FROM EQUIPMENT;

-- name: InsertHolelocation :one
INSERT INTO HOLELOCATION (HOLEID, PROJECTCODE) VALUES :HOLEID, :PROJECTCODE;

-- name: InsertHolelocation2 :one
INSERT INTO HOLELOCATION (HOLEID) VALUES :HOLEID;

-- name: UpdateHolelocation :one
UPDATE HOLELOCATION
   SET HOLEID = :HOLEID,
       PROJECTCODE = :PROJECTCODE
 WHERE HOLEID = :HOLEID
   AND PROJECTCODE = :PROJECTCODE;
