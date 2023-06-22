WITH employees AS (
	SELECT
		e.manager_id,
		e.id AS employee_id,
		contract_contract.id AS site_id,
		contract_contract.name AS site_name,
		e.organization_id AS organization_id,
		p.first_name,
		p.last_name,
		p.photo_url,
		p.email_address,
		pos.name AS position
	FROM
		organization_employee e
	LEFT JOIN organization_person p ON p.id = e.person_id
	LEFT JOIN organization_employee_positions ep ON ep.employee_id = e.id
	LEFT JOIN organization_position pos ON pos.id = ep.position_id
	LEFT JOIN organization_employee_shifts ON e.id = organization_employee_shifts.employee_id
	LEFT JOIN organization_shift ON organization_employee_shifts.shift_id = organization_shift.id
	LEFT JOIN contract_contract ON organization_shift.contract_id = contract_contract.id
		AND contract_contract.delete_date IS NULL
	WHERE
		e.organization_id IN (1)
		AND e.delete_date IS NULL
		AND contract_contract.id IN (35)
),
managers AS (
	SELECT DISTINCT ON (emp.employee_id)
		emp.manager_id,
		emp.employee_id,
		emp.site_id,
		emp.position,
		emp.site_name,
		emp.organization_id,
		emp.first_name,
		emp.last_name,
		emp.photo_url,
		emp.email_address
	FROM
		employees emp
		JOIN organization_employee manager ON emp.employee_id = manager.manager_id
),
managers_nonapprover AS (
	SELECT DISTINCT
		manager.employee_id
	FROM
		managers manager
	WHERE
		NOT EXISTS (
			SELECT
				1
			FROM
				workday_workday w
			WHERE
				w.finalized_by_id = manager.employee_id
				AND w.finalized_on >= (date_trunc('day', '2022-01-01'::timestamp) - INTERVAL '60 days')
		)
),
total_workday AS (
	SELECT
		count(days) AS total_workdays_count,
		manager.employee_id AS id
	FROM
		generate_series(date_trunc('day', timestamp '2022-01-01'), date_trunc('day', timestamp '2022-12-31'), '1 day'::interval) AS days
		JOIN managers manager ON manager.site_id = 35
	GROUP BY
		manager.employee_id
),
approved_workday AS (
	SELECT
		COUNT(DISTINCT work_date) AS total_approved_count,
		managers.employee_id AS id
	FROM
		workday_workday w1
		JOIN managers ON managers.site_id = w1.contract_id
	WHERE
		w1.work_date BETWEEN '2022-01-01' AND '2022-12-31'
		AND w1.finalized_on IS NOT NULL
		AND w1.work_date IS NOT NULL
		AND w1.delete_date IS NULL
		AND EXTRACT(EPOCH FROM (w1.finalized_on - w1.work_date)) <= 59400 + 86400
	GROUP BY
		managers.employee_id
),
messages AS (
	SELECT
		manager.employee_id AS id,
		COUNT(*) AS message_count
	FROM
		message_message
		INNER JOIN managers manager ON message_message.from_employee_id = manager.employee_id
	WHERE
		message_message.from_contract_id IN (SELECT site_id FROM managers)
		AND message_message.sent_date BETWEEN '2022-01-01' AND '2022-12-31'
		AND message_message.delete_date IS NULL
		AND message_message.from_employee_id = manager.employee_id
	GROUP BY
		manager.employee_id
),
inspections AS (
	SELECT
		manager.employee_id AS id,
		COUNT(*) AS inspection_count
	FROM
		inspection_inspection i
		INNER JOIN managers manager ON i.inspected_by_id = manager.employee_id
	WHERE
		i.delete_date IS NULL
		AND i.status = 'COMPLETE'
		AND i.mode IN ('AUDIT', 'INSPECTION')
		AND i.inspection_date BETWEEN '2022-01-01' AND '2022-12-31'
		AND i.contract_id IN (SELECT site_id FROM managers)
		AND i.inspected_by_id = manager.employee_id
	GROUP BY
		manager.employee_id
),
user_hits AS (
	SELECT
		usr.employee_id AS id,
		usr.first_name,
		COUNT(*) AS user_hits_count
	FROM
		user_hits usr
		INNER JOIN managers manager ON usr.employee_id = manager.employee_id
	WHERE
		usr.site_id = 35
		AND date BETWEEN '2022-01-01' AND '2022-12-31'
	GROUP BY
		usr.employee_id,
		usr.first_name
),
last_visit AS (
	SELECT
		employee_id AS id,
		first_name AS last_visit_date
	FROM
		user_hits
	WHERE
		site_id = 35
	GROUP BY
		employee_id, first_name
)
SELECT
	manager.employee_id,
	manager.manager_id,
	manager.site_id AS site_id,
	manager.site_name AS site_name,
	manager.organization_id AS organization_id,
	json_build_object('first_name', manager.first_name, 'last_name', manager.last_name, 'photo_url', manager.photo_url, 'email_address', manager.email_address, 'position', manager.position) AS person,
	COALESCE(messages.message_count, 0) AS message_count,
	CASE
		WHEN manager.employee_id IN (SELECT employee_id FROM managers_nonapprover) THEN -1
		ELSE COALESCE(ROUND((CAST(aw.total_approved_count AS numeric) / COALESCE(tw.total_workdays_count, 1)) * 100, 2), 0)
	END AS approved_hours,
	COALESCE(i.inspection_count, 0) AS audits,
	manager.first_name || ' ' || manager.last_name AS manager_name,
	user_hits.user_hits_count AS user_hits,
	last_visit.last_visit_date AS last_visit
FROM
	managers manager
	LEFT JOIN messages ON messages.id = manager.employee_id
	LEFT JOIN inspections i ON i.id = manager.employee_id
	LEFT JOIN approved_workday aw ON aw.id = manager.employee_id
	LEFT JOIN total_workday tw ON tw.id = manager.employee_id
	LEFT JOIN user_hits ON user_hits.id = manager.employee_id
	LEFT JOIN last_visit ON last_visit.id = manager.employee_id;
