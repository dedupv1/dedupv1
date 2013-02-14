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

#ifndef CALLBACK_H__
#define CALLBACK_H__

#include <base/base.h>

namespace dedupv1 {
namespace base {

/**
 * A callback.
 *
 * New closures can be created with the
 * NewClosure methods either from a function, a
 * method.
 */
template<class RT> class Callback0 {
    public:
        Callback0() {
        }
        virtual ~Callback0() {
        }
        virtual RT Call() = 0;
};

/**
 * A callback with a single parameter.
 */
template<class RT, class P> class Callback1 {
    public:
        Callback1() {
        }
        virtual ~Callback1() {
        }
        virtual RT Call(P p) = 0;
};

/**
 * A callback with two parameters.
 * If there is the need for a callback with more
 * paremeters, tuples should be used.
 */
template<class RT, class P1, class P2> class Callback2 {
    public:
        Callback2() {
        }
        virtual ~Callback2() {
        }
        virtual RT Call(P1 p1, P2 p2) = 0;
};

/**
 * Function callback without parameters
 */
template<class RT> class FunctionCallback0 : public Callback0<RT> {
    DISALLOW_COPY_AND_ASSIGN(FunctionCallback0);

    /**
     * method pointer
     * @return
     */
    RT (*method_)();
    public:

    /**
     * Constructor.
     * Should not be called directly. Use the NewCallback
     * function.
     *
     * @param object
     * @param method
     * @return
     */
    FunctionCallback0(RT (*method)()) {
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~FunctionCallback0() {
    }

    virtual RT Call() {
        RT t = (*this->method_)();
        return t;
    }
};

/**
 * Function callback without parameters
 */
class VoidFunctionCallback0 : public Callback0<void> {
    DISALLOW_COPY_AND_ASSIGN(VoidFunctionCallback0);

    /**
     * method pointer
     * @return
     */
    void (*method_)();
    public:

    /**
     * Constructor.
     * Should not be called directly. Use the NewCallback
     * function.
     *
     * @param object
     * @param method
     * @return
     */
    VoidFunctionCallback0(void (*method)()) {
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~VoidFunctionCallback0() {
    }

    virtual void Call() {
        (*this->method_)();
    }
};


/**
 * Method callback without parameters
 */
template<class RT, class C> class MethodCallback0 : public Callback0<RT> {
    DISALLOW_COPY_AND_ASSIGN(MethodCallback0);

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
     * Should not be called directly. Use the NewCallback
     * function.
     *
     * @param object
     * @param method
     * @return
     */
    MethodCallback0(C* object, RT (C::*method)()) {
        this->object_ = object;
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~MethodCallback0() {
    }

    virtual RT Call() {
        RT t = (this->object_->*this->method_)();
        return t;
    }
};

/**
 * Method callback without parameters
 */
template<class C> class VoidMethodCallback0 : public Callback0<void> {
    DISALLOW_COPY_AND_ASSIGN(VoidMethodCallback0);

    /**
     * Pointer to the object
     */
    C* object_;

    /**
     * method pointer
     * @return
     */
    void (C::*method_)();
    public:

    /**
     * Constructor.
     * Should not be called directly. Use the NewCallback
     * function.
     *
     * @param object
     * @param method
     * @return
     */
    VoidMethodCallback0(C* object, void (C::*method)()) {
        this->object_ = object;
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~VoidMethodCallback0() {
    }

    virtual void Call() {
        (this->object_->*this->method_)();
    }
};

/**
 * Function callback with one parameter
 */
template<class RT, class P> class FunctionCallback1 : public Callback1<RT, P> {
    DISALLOW_COPY_AND_ASSIGN(FunctionCallback1);

    /**
     * method pointer
     * @param
     * @return
     */
    RT (*method_)(P);
    public:

    /**
     * Constructor
     * @param object
     * @param method
     * @return
     */
    FunctionCallback1(RT (*method)(P)) {
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~FunctionCallback1() {
    }

    virtual RT Call(P p) {
        RT t = (*this->method_)(p);
        return t;
    }
};

/**
 * Function callback with one parameter
 */
template<class P> class VoidFunctionCallback1 : public Callback1<void, P> {
    DISALLOW_COPY_AND_ASSIGN(VoidFunctionCallback1);

    /**
     * method pointer
     * @param
     * @return
     */
    void (*method_)(P);
    public:

    /**
     * Constructor
     * @param object
     * @param method
     * @return
     */
    VoidFunctionCallback1(void (*method)(P)) {
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~VoidFunctionCallback1() {
    }

    virtual void Call(P p) {
        (*this->method_)(p);
    }
};


/**
 * Method callback with one parameter
 */
template<class RT, class C, class P> class MethodCallback1 : public Callback1<RT, P> {
    DISALLOW_COPY_AND_ASSIGN(MethodCallback1);

    /**
     * Pointer to the object
     */
    C* object_;

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
     * @return
     */
    MethodCallback1(C* object, RT (C::*method)(P)) {
        this->object_ = object;
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~MethodCallback1() {
    }

    virtual RT Call(P p) {
        RT t = (this->object_->*this->method_)(p);
        return t;
    }
};

/**
 * Method callback with one parameter
 */
template<class C, class P> class VoidMethodCallback1 : public Callback1<void, P> {
    DISALLOW_COPY_AND_ASSIGN(VoidMethodCallback1);

    /**
     * Pointer to the object
     */
    C* object_;

    /**
     * method pointer
     * @param
     * @return
     */
    void (C::*method_)(P);
    public:

    /**
     * Constructor
     * @param object
     * @param method
     * @return
     */
    VoidMethodCallback1(C* object, void (C::*method)(P)) {
        this->object_ = object;
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~VoidMethodCallback1() {
    }

    virtual void Call(P p) {
        (this->object_->*this->method_)(p);
    }
};

/**
 * Internal implementation class for a runnable
 * build from a method with two parameters
 */
template<class RT, class C, class P1, class P2> class MethodCallback2 : public Callback2<RT, P1, P2> {
    DISALLOW_COPY_AND_ASSIGN(MethodCallback2);

    /**
     * Pointer to the object
     */
    C* object_;

    /**
     * method pointer
     * @param
     * @return
     */
    RT (C::*method_)(P1, P2);
    public:

    /**
     * Constructor
     * @param object
     * @param method
     * @return
     */
    MethodCallback2(C* object, RT (C::*method)(P1, P2)) {
        this->object_ = object;
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~MethodCallback2() {
    }

    /**
     * Runs the method with the given parameter.
     * @return
     */
    virtual RT Call(P1 p1, P2 p2) {
        RT t = (this->object_->*this->method_)(p1, p2);
        return t;
    }
};


/**
 * Internal implementation class for a runnable
 * build from a method with two parameters
 */
template<class C, class P1, class P2> class VoidMethodCallback2 : public Callback2<void, P1, P2> {
    DISALLOW_COPY_AND_ASSIGN(VoidMethodCallback2);

    /**
     * Pointer to the object
     */
    C* object_;

    /**
     * method pointer
     * @param
     * @return
     */
    void (C::*method_)(P1, P2);
    public:

    /**
     * Constructor
     * @param object
     * @param method
     * @return
     */
    VoidMethodCallback2(C* object, void (C::*method)(P1, P2)) {
        this->object_ = object;
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~VoidMethodCallback2() {
    }

    /**
     * Runs the method with the given parameter.
     * @return
     */
    virtual void Call(P1 p1, P2 p2) {
        (this->object_->*this->method_)(p1, p2);
    }
};

/**
 * Internal implementation class for a runnable
 * build from a function with two parameters
 */
template<class RT, class P1, class P2> class FunctionCallback2 : public Callback2<RT, P1, P2> {
    DISALLOW_COPY_AND_ASSIGN(FunctionCallback2);

    /**
     * method pointer
     * @param
     * @return
     */
    RT (*method_)(P1, P2);
    public:

    /**
     * Constructor
     * @param object
     * @param method
     * @return
     */
    FunctionCallback2(RT (*method)(P1, P2)) {
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~FunctionCallback2() {
    }

    /**
     * Runs the method with the given parameter.
     * @return
     */
    virtual RT Call(P1 p1, P2 p2) {
        RT t = (*this->method_)(p1, p2);
        return t;
    }
};

/**
 * Internal implementation class for a runnable
 * build from a function with two parameters
 */
template<class P1, class P2> class VoidFunctionCallback2 : public Callback2<void, P1, P2> {
    DISALLOW_COPY_AND_ASSIGN(VoidFunctionCallback2);

    /**
     * method pointer
     * @param
     * @return
     */
    void (*method_)(P1, P2);
    public:

    /**
     * Constructor
     * @param object
     * @param method
     * @return
     */
    VoidFunctionCallback2(void (*method)(P1, P2)) {
        this->method_ = method;
    }

    /**
     * Destructor
     * @return
     */
    virtual ~VoidFunctionCallback2() {
    }

    /**
     * Runs the method with the given parameter.
     * @return
     */
    virtual void Call(P1 p1, P2 p2) {
        (*this->method_)(p1, p2);
    }
};


/**
 * returns a new callback without a parameter
 * @param object
 * @param method
 * @return
 */
template <class RT, class C> Callback0<RT>* NewCallback(C* object, RT (C::*method)()) {
        return new MethodCallback0<RT,C>(object, method);
}

/**
 * returns a new callback with one parameter.
 *
 * @param object
 * @param method
 * @return
 */
template <class RT, class C, class P> Callback1<RT, P>* NewCallback(C* object, RT (C::*method)(P)) {
        return new MethodCallback1<RT,C,P>(object, method);
}

/**
 * returns a new callback with two parameters.
 * @param object
 * @param method
 * @return
 */
template <class RT, class C, class P1, class P2> Callback2<RT, P1, P2>* NewCallback(C* object, RT (C::*method)(P1, P2)) {
        return new MethodCallback2<RT,C,P1,P2>(object, method);
}

/**
 * returns a new callback without a parameter
 * @param object
 * @param method
 * @return
 */
template <class RT> Callback0<RT>* NewCallback(RT (*method)()) {
        return new FunctionCallback0<RT>(method);
}

/**
 * returns a new callback with one parameter.
 *
 * @param object
 * @param method
 * @return
 */
template <class RT, class P> Callback1<RT, P>* NewCallback(RT (*method)(P)) {
        return new FunctionCallback1<RT,P>(method);
}

/**
 * returns a new callback with two parameters.
 * @param object
 * @param method
 * @return
 */
template <class RT, class P1, class P2> Callback2<RT, P1, P2>* NewCallback(RT (*method)(P1, P2)) {
        return new FunctionCallback2<RT,P1,P2>(method);
}




/**
 * returns a new callback without a parameter
 * @param object
 * @param method
 * @return
 */
template <class C> Callback0<void>* NewVoidCallback(C* object, void (C::*method)()) {
        return new VoidMethodCallback0<C>(object, method);
}

/**
 * returns a new callback with one parameter.
 *
 * @param object
 * @param method
 * @return
 */
template <class C, class P> Callback1<void, P>* NewVoidCallback(C* object, void (C::*method)(P)) {
        return new VoidMethodCallback1<C,P>(object, method);
}



/**
 * returns a new callback with two parameters.
 * @param object
 * @param method
 * @return
 */
template <class C, class P1, class P2> Callback2<void, P1, P2>* NewVoidCallback(C* object, void (C::*method)(P1, P2)) {
        return new VoidMethodCallback2<C,P1,P2>(object, method);
}

/**
 * returns a new callback without a parameter
 * @param object
 * @param method
 * @return
 */
Callback0<void>* NewVoidCallback(void (*method)());

/**
 * returns a new callback with one parameter.
 *
 * @param object
 * @param method
 * @return
 */
template <class P> Callback1<void, P>* NewVoidCallback(void (*method)(P)) {
        return new VoidFunctionCallback1<P>(method);
}

/**
 * returns a new callback with two parameters.
 * @param object
 * @param method
 * @return
 */
template <class P1, class P2> Callback2<void, P1, P2>* NewVoidCallback(void (*method)(P1, P2)) {
        return new VoidFunctionCallback2<P1,P2>(method);
}


}
}

#endif // CALLBACK_H__
