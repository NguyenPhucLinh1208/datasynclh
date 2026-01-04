package com.viettel.sync.model.target;
import com.viettel.sync.core.Annotations.*;

@Table(name = "D_TIME_PARAM_CONFIG")
public record TargetTimeParam(
        @Id @Column(name = "NAME") String name,
        @Column(name = "ADD_DAY") Integer addDay,
        @Column(name = "ADD_MON") Integer addMon,
        @Column(name = "ADD_YEAR") Integer addYear,
        @Column(name = "FORMAT") String format,
        @Column(name = "EXTEND_FORMAT") String extendFormat,
        @Column(name = "ADD_MIN") Integer addMin,
        @Column(name = "ADD_HOUR") Integer addHour
) {}