{#
    This macro returns a description for the payment_type column
#}

{% macro get_payment_desc(payment_type) -%}
    case {{ payment_type }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Unknown'
        when 5 then 'Voided trip'
    end
{%- endmacro %}