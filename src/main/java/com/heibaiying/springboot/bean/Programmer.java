package com.heibaiying.springboot.bean;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
// 需要实现序列化接口
public class Programmer implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String name;

    private int age;

    private float salary;

    private Date birthday;
}
