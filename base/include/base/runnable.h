/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008 Dirk Meister
 * (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
 * (C) 2012 Dirk Meister, Johannes Gutenberg University Mainz
 *
 * This file is part of dedupv1.
 *
 * dedupv1 is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * dedupv1 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with dedupv1. If not, see http://www.gnu.org/licenses/.
 */

#ifndef RUNNABLE_H__
#define RUNNABLE_H__

#include <base/base.h>

namespace dedupv1 {
namespace base {

/**
 * A runnable is a kind of closure that
 * can be run (a single time).
 *
 * New runnables can be created with the
 * NewRunnable methods either from a function, a
 * method.
 */
template<class RT> class Runnable {
    public:
        /**
         * Constructor
         * @return
         */
        Runnable() {
        }

        /**
         * Destructor
         * @return
         */
        virtual ~Runnable() {
        }

        /**
         * Runs the runnable
         * @return
         */
        virtual RT Run() = 0;
};

/**
 * A scoped runnable encapsulates a runnable so that
 * the runnable is called when the scoped runnable
 * leaves the scope.
 */
template<class RT> class ScopedRunnable {
        /**
         * pointer to a runnable
         */
        Runnable<RT>* runnable_;
    public:
        /**
         * Constructor for a new scoped runnable.
         *
         * @param runnable
         * @return
         */
        explicit ScopedRunnable(Runnable<RT>* runnable) {
            this->runnable_ = runnable;
        }

        /**
         * Destructor that calls the runnable.
         *
         * @return
         */
        virtual ~ScopedRunnable() {
            if (runnable_) {
                runnable_->Run();
            }
        }
};

/**
 * Internal implementation class for a runnable
 * build from a method with zero parameters.
 */
template<class RT, class C> class MethodRunnable : public Runnable<RT> {
    DISALLOW_COPY_AND_ASSIGN(MethodRunnable);

    /**
     * Pointer to the object
     */
    C* object_;

    /**
     * method pointer
     * @return
     */
    RT (C::*method_)();
    public:

    /**
     * Constructor.
     * Should not be called directly. Use the NewRunnable
     * function.
     *
     * @param object
     * @param method
     * @return
     */
    MethodRunnable(C* object, RT (C::*method)()) {
        this->object_ = object;
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~MethodRunnable() {
    }

    /**
     * Runs the method on the given object.
     *
     * @return
     */
    virtual RT Run() {
        RT t = (this->object_->*this->method_)();
        delete this;
        return t;
    }
};

/**
 * Internal implementation class for a runnable
 * build from a method with a single parameter
 */
template<class RT, class C, class P> class MethodParameterRunnable : public Runnable<RT> {
    DISALLOW_COPY_AND_ASSIGN(MethodParameterRunnable);

    /**
     * Pointer to the object
     */
    C* object_;

    /**
     * The parameter
     */
    P parameter_;

    /**
     * method pointer
     * @param
     * @return
     */
    RT (C::*method_)(P);
    public:

    /**
     * Constructor
     * @param object
     * @param method
     * @param p
     * @return
     */
    MethodParameterRunnable(C* object, RT (C::*method)(P), P p) {
        this->object_ = object;
        this->method_ = method;
        this->parameter_ = p;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~MethodParameterRunnable() {
    }

    /**
     * Runs the method with the given parameter.
     * @return
     */
    virtual RT Run() {
        RT t = (this->object_->*this->method_)(this->parameter_);
        delete this;
        return t;
    }
};

/**
 * Internal implementation class for a runnable
 * build from a method with a two parameter
 */
template<class RT, class C, class P1, class P2> class Method2ParameterRunnable : public Runnable<RT> {
    DISALLOW_COPY_AND_ASSIGN(Method2ParameterRunnable);

    /**
     * Pointer to the object
     */
    C* object_;

    /**
     * The parameter
     */
    P1 parameter1_;
    P2 parameter2_;

    /**
     * method pointer
     * @param
     * @return
     */
    RT (C::*method_)(P1, P2);
    public:

    /**
     * Constructor.
     *
     * @param object
     * @param method
     * @param p1 first parameter
     * @param p2 second parameter
     * @return
     */
    Method2ParameterRunnable(C* object, RT (C::*method)(P1, P2), P1 p1, P2 p2) {
        this->object_ = object;
        this->method_ = method;
        this->parameter1_ = p1;
        this->parameter2_ = p2;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~Method2ParameterRunnable() {
    }

    /**
     * Runs the method with the given parameter.
     * @return
     */
    virtual RT Run() {
        RT t = (this->object_->*this->method_)(this->parameter1_, this->parameter2_);
        delete this;
        return t;
    }
};

/**
 * Internal implementation class for a runnable
 * build from a method with a three parameter
 */
template<class RT, class C, class P1, class P2, class P3> class Method3ParameterRunnable : public Runnable<RT> {
    DISALLOW_COPY_AND_ASSIGN(Method3ParameterRunnable);

    /**
     * Pointer to the object
     */
    C* object_;

    /**
     * The parameter
     */
    P1 parameter1_;
    P2 parameter2_;
    P3 parameter3_;

    /**
     * method pointer
     * @param
     * @return
     */
    RT (C::*method_)(P1, P2, P3);
    public:

    /**
     * Constructor.
     *
     * @param object
     * @param method
     * @param p1 first parameter
     * @param p2 second parameter
     * @param p3 third parameter.
     * @return
     */
    Method3ParameterRunnable(C* object, RT (C::*method)(P1, P2, P3), P1 p1, P2 p2, P3 p3) {
        this->object_ = object;
        this->method_ = method;
        this->parameter1_ = p1;
        this->parameter2_ = p2;
        this->parameter3_ = p3;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~Method3ParameterRunnable() {
    }

    /**
     * Runs the method with the given parameter.
     * @return
     */
    virtual RT Run() {
        RT t = (this->object_->*this->method_)(this->parameter1_, this->parameter2_, this->parameter3_);
        delete this;
        return t;
    }
};

/**
 * Internal implementation class for a runnable
 * build from a method with a four parameter
 */
template<class RT, class C, class P1, class P2, class P3, class P4> class Method4ParameterRunnable : public Runnable<RT> {
    DISALLOW_COPY_AND_ASSIGN(Method4ParameterRunnable);

    /**
     * Pointer to the object
     */
    C* object_;

    /**
     * The parameter
     */
    P1 parameter1_;
    P2 parameter2_;
    P3 parameter3_;
    P4 parameter4_;

    /**
     * method pointer
     * @param
     * @return
     */
    RT (C::*method_)(P1, P2, P3, P4);
    public:

    /**
     * Constructor
     * @param object
     * @param method
     * @param p1 first parameter
     * @param p2 second parameter
     * @param p3 third parameter
     * @param p4 fourth parameter
     * @return
     */
    Method4ParameterRunnable(C* object, RT (C::*method)(P1, P2, P3, P4), P1 p1, P2 p2, P3 p3, P4 p4) {
        this->object_ = object;
        this->method_ = method;
        this->parameter1_ = p1;
        this->parameter2_ = p2;
        this->parameter3_ = p3;
        this->parameter4_ = p4;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~Method4ParameterRunnable() {
    }

    /**
     * Runs the method with the given parameter.
     * @return
     */
    virtual RT Run() {
        RT t = (this->object_->*this->method_)(this->parameter1_, this->parameter2_, this->parameter3_, this->parameter4_);
        delete this;
        return t;
    }
};

/**
 * Internal implementation class for a runnable
 * build from a function with zero parameters.
 */
template<class RT> class FunctionRunnable : public Runnable<RT> {
    DISALLOW_COPY_AND_ASSIGN(FunctionRunnable);

    /**
     * Function pointer.
     * @return
     */
    RT (*function_)();
    public:

    /**
     * Constructor
     * @param function
     * @return
     */
    FunctionRunnable(RT (*function)()) {
        this->function_ = function;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~FunctionRunnable() {
    }

    /**
     * Runs the function.
     *
     * @return
     */
    virtual RT Run() {
        RT t = (*this->function_)();
        delete this;
        return t;
    }
};

/**
 * Internal implementation class for a runnable
 * build from a function with one parameters.
 */
template<class RT, class P> class FunctionParameterRunnable : public Runnable<RT> {
    DISALLOW_COPY_AND_ASSIGN(FunctionParameterRunnable);

    /**
     * Function pointer
     */
    RT (*function_)(P);

    /**
     * Parameter to call the function with
     */
    P parameter_;
    public:

    /**
     * Constructor.
     * @param function
     * @param p
     * @return
     */
    FunctionParameterRunnable(RT (*function)(P), P p) {
        this->function_ = function;
        this->parameter_ = p;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~FunctionParameterRunnable() {
    }

    /**
     * Calls the function with the given parameter
     * @return
     */
    virtual RT Run() {
        RT t = (*this->function_)(this->parameter_);
        delete this;
        return t;
    }
};

/**
 * Internal implementation class for a runnable
 * build from a function with two parameters.
 */
template<class RT, class P1, class P2> class Function2ParameterRunnable : public Runnable<RT> {
    DISALLOW_COPY_AND_ASSIGN(Function2ParameterRunnable);

    /**
     * Function pointer
     */
    RT (*function_)(P1, P2);

    /**
     * Parameter to call the function with
     */
    P1 parameter1_;
    P2 parameter2_;
    public:

    /**
     * Constructor.
     *
     * @param function
     * @param p1 first parameter
     * @param p2 second parameter
     * @return
     */
    Function2ParameterRunnable(RT (*function)(P1, P2), P1 p1, P2 p2) {
        this->function_ = function;
        this->parameter1_ = p1;
        this->parameter2_ = p2;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~Function2ParameterRunnable() {
    }

    /**
     * Calls the function with the given parameter
     * @return
     */
    virtual RT Run() {
        RT t = (*this->function_)(this->parameter1_, this->parameter2_);
        delete this;
        return t;
    }
};

/**
 * Internal implementation class for a runnable
 * build from a function with three parameters.
 */
template<class RT, class P1, class P2, class P3> class Function3ParameterRunnable : public Runnable<RT> {
    DISALLOW_COPY_AND_ASSIGN(Function3ParameterRunnable);

    /**
     * Function pointer
     */
    RT (*function_)(P1, P2, P3);

    /**
     * Parameter to call the function with
     */
    P1 parameter1_;

    /**
     * Second parameter to call the function with
     */
    P2 parameter2_;

    /**
     * Third parameter to call the functions with.
     */
    P3 parameter3_;
    public:

    /**
     * Constructor.
     *
     * @param function
     * @param p1 first parameter
     * @param p2 second parameter
     * @param p3 third parameter
     * @return
     */
    Function3ParameterRunnable(RT (*function)(P1, P2, P3), P1 p1, P2 p2, P3 p3) {
        this->function_ = function;
        this->parameter1_ = p1;
        this->parameter2_ = p2;
        this->parameter3_ = p3;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~Function3ParameterRunnable() {
    }

    /**
     * Calls the function with the given parameter
     * @return
     */
    virtual RT Run() {
        RT t = (*this->function_)(this->parameter1_, this->parameter2_, this->parameter3_);
        delete this;
        return t;
    }
};

/**
 * \relates Runnable
 *
 * @param object
 * @param method
 * @return
 */
template <class RT, class C> Runnable<RT>* NewRunnable(C* object, RT (C::*method)()) {
        return new MethodRunnable<RT,C>(object, method);
}

/**
 * \relates Runnable
 *
 * Creates a new runnable with a single parameter
 *
 * @param object
 * @param method
 * @param p
 * @return
 */
template <class RT, class C, class P> Runnable<RT>* NewRunnable(C* object, RT (C::*method)(P), P p) {
        return new MethodParameterRunnable<RT,C,P>(object, method, p);
}

/**
 * \relates Runnable
 *
 * Creates a new runnable with two parameters.
 *
 * @param object
 * @param method
 * @param p1 first parameter
 * @param p2 second parameter
 * @return
 */
template <class RT, class C, class P1, class P2> Runnable<RT>* NewRunnable(C* object, RT (C::*method)(P1, P2), P1 p1, P2 p2) {
        return new Method2ParameterRunnable<RT,C,P1,P2>(object, method, p1, p2);
}

/**
 * \relates Runnable
 *
 * Creates a new runnable with three parameters
 *
 * @param object
 * @param method
 * @param p1 first parameter
 * @param p2 second parameter
 * @param p3 third parameter
 * @return
 */
template <class RT, class C, class P1, class P2, class P3> Runnable<RT>* NewRunnable(C* object, RT (C::*method)(P1, P2, P3), P1 p1, P2 p2, P3 p3) {
        return new Method3ParameterRunnable<RT,C,P1, P2, P3>(object, method, p1, p2, p3);
}

/**
 * \relates Runnable
 *
 * Creates a new runnable with four parameters
 *
 * @param object
 * @param method
 * @param p1 first parameter
 * @param p2 second parameter
 * @param p3 third parameter
 * @param p4 fourth parameter
 * @return
 */
template <class RT, class C, class P1, class P2, class P3, class P4> Runnable<RT>* NewRunnable(C* object, RT (C::*method)(P1, P2, P3, P4), P1 p1, P2 p2, P3 p3, P4 p4) {
        return new Method4ParameterRunnable<RT,C,P1, P2, P3, P4>(object, method, p1, p2, p3, p4);
}

/**
 * \relates Runnable
 *
 * Creates a new runnable instance. The client is responsible to free the runnable.
 *
 *
 * @param function
 * @return
 */
template <class RT> Runnable<RT>* NewRunnable(RT (*function)()) {
        return new FunctionRunnable<RT>(function);
}

/**
 * \relates Runnable
 *
 * Creates a new runnable instance. The client is responsible to free the runnable.
 *
 * @param function
 * @param p
 * @return
 */
template <class RT, class P> Runnable<RT>* NewRunnable(RT (*function)(P), P p) {
        return new FunctionParameterRunnable<RT,P>(function, p);
}

/**
 * \relates Runnable
 *
 * Creates a new runnable instance. The client is responsible to free the runnable.
 *
 * @param function
 * @param p1 first parameter
 * @param p2 second parameter
 * @return
 */
template <class RT, class P1, class P2> Runnable<RT>* NewRunnable(RT (*function)(P1,P2), P1 p1, P2 p2) {
        return new Function2ParameterRunnable<RT,P1,P2>(function, p1, p2);
}

/**
 * \relates Runnable
 *
 * Creates a new runnable instance. The client is responsible to free the runnable.
 *
 * @param function
 * @param p1 first parameter
 * @param p2 second parameter
 * @param p3 third parameter
 * @return
 */
template <class RT, class P1, class P2, class P3> Runnable<RT>* NewRunnable(RT (*function)(P1,P2,P3), P1 p1, P2 p2, P3 p3) {
        return new Function3ParameterRunnable<RT,P1,P2,P3>(function, p1, p2, p3);
}
}
}

#endif  // RUNNABLE_H__
