#include <iostream>

#include "parse/stream_tokenizer.h"
#include "parse/parser.h"
#include "gtest/gtest.h"

namespace SimpleDB {

TEST(ParseTest, ParserTest) {
    std::string s1 = "select    C from      T where A  =B";
    std::string s2 = "insert into   T(A,    B, C)   values('a'    , 'b', 'c')";
    std::string s3 = "delete  from T where A=B";
    std::string s4 = "update T set A='a' where B=C";
    std::string s5 = "create table T(A int, B varchar(9))";
    std::string s6 = "create view V as " + s1;
    std::string s7 = "create index I on T(A)";

    Parser p1(s1);
    auto ptr1 = p1.ParseQuery();
    std::cout << ptr1->ToString() << std::endl;

    Parser p2(s2);
    auto ptr2 = p2.ParseUpdateCmd();
    std::cout << ptr2->ToString() << std::endl;

    Parser p3(s3);
    auto ptr3 = p3.ParseUpdateCmd();
    std::cout << ptr3->ToString() << std::endl;

    Parser p4(s4);
    auto ptr4 = p4.ParseUpdateCmd();
    std::cout << ptr4->ToString() << std::endl;

    Parser p5(s5);
    auto ptr5 = p5.ParseUpdateCmd();
    std::cout << ptr5->ToString() << std::endl;

    Parser p6(s6);
    auto ptr6 = p6.ParseUpdateCmd();
    std::cout << ptr6->ToString() << std::endl;

    Parser p7(s7);
    auto ptr7 = p7.ParseUpdateCmd();
    std::cout << ptr7->ToString() << std::endl;
}

TEST(ParseTest, AcTionTest) {
    std::string s1 = "select    C from      T where A  =B";
    std::string s2 = "insert into   T(A,    B, C)   values('a'    , 'b', 'c')";
    std::string s3 = "delete  from T where A=B";
    std::string s4 = "update T set A='a' where B=C";
    std::string s5 = "create table T(A int, B varchar(9))";
    std::string s6 = "create view V as " + s1;
    std::string s7 = "create index I on T(A)";

    Parser p1(s1);
    std::cout << "Case 1" << std::endl;
    std::cout << "In: " << s1 << std::endl;
    std::cout << "Out: " << p1.ParseQuery()->ToString() << std::endl;

    Parser p2(s2);
    std::cout << "Case 2" << std::endl;
    std::cout << "In: " << s2 << std::endl;
    std::cout << "Out op: "
                << static_cast<Object::Operation>(p2.ParseUpdateCmd()->GetOP())
                << std::endl;
    ;

    Parser p3(s3);
    std::cout << "Case 3" << std::endl;
    std::cout << "In: " << s3 << std::endl;
    std::cout << "Out op: "
                << static_cast<Object::Operation>(p3.ParseUpdateCmd()->GetOP())
                << std::endl;
    ;

    Parser p4(s4);
    std::cout << "Case 4" << std::endl;
    std::cout << "In: " << s4 << std::endl;
    std::cout << "Out op: "
                << static_cast<Object::Operation>(p4.ParseUpdateCmd()->GetOP())
                << std::endl;
    ;

    Parser p5(s5);
    std::cout << "Case 5" << std::endl;
    std::cout << "In: " << s5 << std::endl;
    std::cout << "Out op: "
                << static_cast<Object::Operation>(p5.ParseUpdateCmd()->GetOP())
                << std::endl;
    ;

    Parser p6(s6);
    std::cout << "Case 6" << std::endl;
    std::cout << "In: " << s6 << std::endl;
    std::cout << "Out op: "
                << static_cast<Object::Operation>(p6.ParseUpdateCmd()->GetOP())
                << std::endl;
    ;

    Parser p7(s7);
    std::cout << "Case 7" << std::endl;
    std::cout << "In: " << s7 << std::endl;
    std::cout << "Out op: "
                << static_cast<Object::Operation>(p7.ParseUpdateCmd()->GetOP())
                << std::endl;
    ;
}

} // namespace SimpleDB
